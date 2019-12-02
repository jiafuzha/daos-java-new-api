package com.intel.daos.client;

import java.nio.ByteBuffer;

public class StatAttributes {

  private final long objId;

  private int mode;

  private long uid;

  private long gid;

  private long length;

  private long accessTime;

  private long modifyTime;

  private final long createTime;

  private final boolean file;

  protected StatAttributes(ByteBuffer buffer){
    objId = buffer.getLong();
    mode = buffer.getInt();
    uid = buffer.getLong();
    gid = buffer.getLong();
    length = buffer.getLong();
    accessTime = buffer.getLong();
    modifyTime = buffer.getLong();
    createTime = buffer.getLong();
    file = buffer.get() > 0;
  }

  public int getMode() {
    return mode;
  }

  public long getObjId() {
    return objId;
  }

  public long getAccessTime() {
    return accessTime;
  }

  public long getCreateTime() {
    return createTime;
  }

  public long getGid() {
    return gid;
  }

  public long getLength() {
    return length;
  }

  public long getModifyTime() {
    return modifyTime;
  }

  public long getUid() {
    return uid;
  }

  public boolean isFile() {
    return file;
  }

  public static int objectSize(){
    return 61;
  }
}
