package com.intel.daos.client;

import org.apache.commons.lang.ObjectUtils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public final class DaosFsClient {

  private String poolId;

  private String contId;

  private long poolPtr;

  private long contPtr;

  private long dfsPtr;

  private DaosFsClientBuilder builder;

  private volatile boolean inited;

  //make it non-daemon so that all DAOS file object can be released
  private static final Executor cleanerExe = Executors.newSingleThreadExecutor((r) -> {
    Thread thread = new Thread(r, "DAOS file object cleaner thread");
    thread.setDaemon(false);
    return thread;
  });

  //keyed by poolId+contId
  private static final Map<String, DaosFsClient> pcFsMap = new ConcurrentHashMap<>();

  static {
    loadLib();
    loadErrorCode();
    cleanerExe.execute(new Cleaner.CleanerTask());
    ShutdownHookManager.addHook(() -> daosFinalize());
  }

  private static void loadLib() {}

  private static void loadErrorCode(){}

  private DaosFsClient(String poolId, String contId, DaosFsClientBuilder builder) {
    this.poolId = poolId;
    this.contId = contId;
    this.builder = builder;
  }

  private DaosFsClient(String poolId, DaosFsClientBuilder builder) {
    this(poolId, null, builder);
  }

  private DaosFsClient(DaosFsClientBuilder builder) {
    this(null, null, builder);
  }

  private void init() {
    if (poolId == null) {
      poolId = createPool(builder);
    }
    poolPtr = daosOpenPool(poolId, builder.serverGroup,
            builder.poolMode,
            builder.poolScmSize);

    if (contId == null) {
      contId = createContainer(poolPtr);
    }
    contPtr = daosOpenCont(poolPtr, contId, builder.poolMode);
    dfsPtr = mountFileSystem(poolPtr, contPtr, builder.readOnlyFs);

    cleanerExe.execute(new Cleaner.CleanerTask());
    ShutdownHookManager.addHook(() -> disconnect());
    inited = true;
  }

  public long getDfsPtr() {
    return dfsPtr;
  }

  public static String createPool(DaosFsClientBuilder builder) {
    return daosCreatePool(builder.serverGroup,
                          builder.poolMode,
                          builder.poolScmSize,
                          builder.poolNvmeSize);
  }

  public static String createContainer(long poolPtr) {
    return daoCreateContainer(poolPtr);
  }

  public static synchronized long mountFileSystem(long poolPtr, long contPtr, boolean readOnly) {
    return dfsMountFs(poolPtr, contPtr, readOnly);
  }

  public void disconnect() {
    if (inited && dfsPtr != 0) {
      dfsUnmountFs(dfsPtr);
      daosCloseContainer(contPtr);
      daosClosePool(poolPtr);
    }
    inited = false;
    pcFsMap.remove(poolId+contId);
  }

  public DaosFile newFile(String path) {
    return newFile(path, builder.defFileAccessFlag, builder.defFileMode);
  }

  public DaosFile newFile(String path, int accessFlags, int mode) {
    DaosFile daosFile = new DaosFile(path, this);
    daosFile.setAccessFlags(accessFlags);
    daosFile.setMode(mode);
    return daosFile;
  }

  public DaosFile newFile(String parent, String path){
    return newFile(parent, path, builder.defFileAccessFlag, builder.defFileMode);
  }

  public DaosFile newFile(String parent, String path, int accessFlags, int mode) {
    DaosFile daosFile = new DaosFile(parent, path, this);
    daosFile.setAccessFlags(accessFlags);
    daosFile.setMode(mode);
    return daosFile;
  }

  public DaosFile newFile(DaosFile parent, String path){
    return newFile(parent, path, builder.defFileAccessFlag, builder.defFileMode);
  }

  public DaosFile newFile(DaosFile parent, String path, int accessFlags, int mode) {
    DaosFile daosFile = new DaosFile(parent, path, this);
    daosFile.setAccessFlags(accessFlags);
    daosFile.setMode(mode);
    return daosFile;
  }

  //non-native methods
  public void move(String srcPath, String destPath){
    move(dfsPtr, srcPath, destPath);
  }

  public void delete(String path){
    path = DaosUtils.normalize(path);
    String[] pc = DaosUtils.parsePath(path);
    delete(dfsPtr, pc.length==2 ? pc[0]:null, pc[1]);
  }

  public void mkdir(String path, boolean recursive){
    mkdir(dfsPtr, path, recursive);
  }

  //common methods
  private native void move(long dfsPtr, String srcPath, String destPath);
  private native void mkdir(long dfsPtr, String path, boolean recursive);


  //methods for DaosFile
  protected native long createNewFile(long dfsPtr, String parentPath, String path, int mode, int accessFlags, int id);

  protected native boolean delete(long dfsPtr, String parentPath, String path);


  //DAOS corresponding methods
  protected static native String daosCreatePool(String serverGroup, int mode, long scmSize, long nvmeSize);

  protected static native String daoCreateContainer(long poolPtr);

  protected static native long daosOpenPool(String poolId, String serverGroup, int mode, long scmSize);

  protected static native long daosOpenCont(long poolPtr, String contId, int mode);

  protected static native void daosCloseContainer(long contPtr);

  protected static native void daosClosePool(long poolPtr);



  //DAOS FS corresponding methods
  protected native int dfsSetPrefix(long dfsPtr, String prefix);

  protected native long dfsLookup(long dfsPtr, long parentObjId, String name, int flags, long bufferAddress);

  protected native long dfsLookup(long dfsPtr, String parentPath, String name, int flags, long bufferAddress);

  protected native long dfsGetSize(long dfsPtr, long objId);

  protected native long dfsOpen(long dfsPtr, long parentObjId, String name, int mode, int flags, int objectClassId,
                             long size, String symLink);

  protected native long dfsDup(long dfsPtr, long objId, int flags);

  protected native long dfsRelease(long dfsPtr, long objId);

  //return actual read
  protected native long dfsRead(long dfsPtr, long objId, long bufferAddress, long offset, long len, int eventNo);

  protected native long dfsWrite(long dfsPtr, long objId, long bufferAddress, long offset, long len,
                              int eventNo);

  protected native long dfsPunch(long dfsPtr, long objId, long offset, long len);

  //call dfs_readdir or dfs_iterate
  protected native String[] dfsReadDir(long dfsPtr, long objId, int maxEntries);

  protected native long dfsMkdir(long dfsPtr, long parentObjId, String name, int mode);

  //return object id of removed object
  protected native long dfsRemove(long dfsPtr, long parentObjId, String name, boolean force);

  //return old object id of moved object
  protected native long dfsMove(long dfsPtr, long parentObjId, String name, long newParentId, String newName);

  protected native void dfsExchange(long dfsPtr, long parentObjId1, String name1, long parentObjId2, String name2);

  protected native StatAttributes dfsStat(long dfsPtr, long parentObjId, String name);

  protected native StatAttributes dfsOpenObjStat(long dfsPtr, long objId);

  protected native void dfsSetOpenObjStat(long dfsPtr, long objId, StatAttributes attributes, int flags);

  protected native boolean dfsCheckAccess(long dfsPtr, long parentObjId, String name, int mask);

  protected native void dfsChangeMode(long dfsPtr, long parentObjId, String name, int mode);

  protected native void dfsSync(long dfsPtr);

  protected native void dfsSetExtAttr(long dfsPtr, long objId, String name, String value, int flags);

  protected native String dfsGetExtAttr(long dfsPtr, long objId, String name);

  protected native String dfsRemoveExtAttr(long dfsPtr, long objId, String name);

  protected native String dfsListExtAttr(long dfsPtr, long objId);


  protected static native long dfsGetChunkSize(long objId);

  protected static native int dfsGetMode(long objId);

  protected static native String dfsGetSymLink(long objId);

  private static native String dfsCreateContainer(String poolId, int svc);

  private static native long dfsMountFs(long poolPtr, long contPtr, boolean readOnly);

  private static native void dfsUnmountFs(long dfsPtr);

  private static native long dfsMountFsOnRootCont(long handlerId);

  private static native long dfsUnmountRootContFs(long dfsPtr);

  private static native void daosFinalize();

  public static class DaosFsClientBuilder{

    private String poolId;
    private String contId;
    private String svc;
    private String serverGroup;
    private int poolMode;
    private long poolScmSize;
    private long poolNvmeSize;
    private int defFileAccessFlag;
    private int defFileMode;
    private DaosObjectType defFileObjType = DaosObjectType.OC_SX;
    private boolean readOnlyFs;
    private boolean shareFsClient = true;

    public DaosFsClientBuilder poolId(String poolId){
      this.poolId = poolId;
      return this;
    }

    public DaosFsClientBuilder containerId(String contId){
      this.contId = contId;
      return this;
    }

    public DaosFsClientBuilder svc(String svc){
      this.svc = svc;
      return this;
    }

    public DaosFsClientBuilder serverGroup(String serverGroup){
      this.serverGroup = serverGroup;
      return this;
    }

    public DaosFsClientBuilder poolMode(int poolMode){
      this.poolMode = poolMode;
      return this;
    }

    public DaosFsClientBuilder poolScmSize(long poolScmSize){
      this.poolScmSize = poolScmSize;
      return this;
    }

    public DaosFsClientBuilder poolNvmeSize(long poolNvmeSize){
      this.poolNvmeSize = poolNvmeSize;
      return this;
    }

    public DaosFsClientBuilder defFileAccessFlag(int defFileAccessFlag){
      this.defFileAccessFlag = defFileAccessFlag;
      return this;
    }

    public DaosFsClientBuilder defFileMode(int defFileMode){
      this.defFileMode = defFileMode;
      return this;
    }

    public DaosFsClientBuilder defFileType(DaosObjectType defFileObjType){
      this.defFileObjType = defFileObjType;
      return this;
    }

    public DaosFsClientBuilder readOnlyFs(boolean readOnlyFs){
      this.readOnlyFs = readOnlyFs;
      return this;
    }

    public DaosFsClientBuilder shareFsClient(boolean shareFsClient){
      this.shareFsClient = shareFsClient;
      return this;
    }

    @Override
    public DaosFsClientBuilder clone(){
      return (DaosFsClientBuilder)ObjectUtils.clone(this);
    }

    public DaosFsClient build(){
      DaosFsClientBuilder copied = this.clone();
      DaosFsClient client;
      if(poolId != null){
        if(contId != null){
          client = getClientForCont(copied);
        }else {
          client = new DaosFsClient(poolId, copied);
        }
      }else {
        client = new DaosFsClient(copied);
      }
      if(!client.inited) {
        client.init();
      }
      return client;
    }

    private DaosFsClient getClientForCont(DaosFsClientBuilder builder) {
      DaosFsClient client;
      if(!builder.shareFsClient){
        return new DaosFsClient(poolId, contId, builder);
      }
      //check existing client
      String key = poolId + contId;
      client = pcFsMap.get(key);
      if(client == null) {
        client = new DaosFsClient(poolId, contId, builder);
        pcFsMap.putIfAbsent(key, client);
      }
      return pcFsMap.get(key);
    }
  }
}
