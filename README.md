# Daos Java Interface 

### 1. Introduction
This is a simple Java interface of [DAOS](https://github.com/daos-stack/daos). Currently it only wraps a small subset of DAOS, and is still under development. Please also pay attention that it might not support the latest version of DAOS.

### 2. Build
First make sure environment variable DAOS_HOME is set to your install path of DAOS. Then you can use `mvn package` to build the project. Notice the tests require a running `daos_server` and `daos_agent`, please consult [here](https://github.com/daos-stack/daos/blob/master/doc/quickstart.md) to see how to run them. Furthermore, you need to set your pool uuid in the tests. You can also specify `-DskipTests` to skip the tests. 

### 3. Manual
Make sure you have a running `daos_server` and `daos_agent`, otherwise you may get error code -1006 or -1026. You can go [here](https://github.com/daos-stack/cart/blob/master/src/include/gurt/errno.h) to check meaning of error code. 

This package provides some abstraction to improve usability and to provide thread safety. You can find corresponding classes for many concepts in DAOS, such as **DaosPool**, **DaosContainer**, etc. Supported APIs are objects and POSIX dfs. High-level API like daos key-value and array are not supported now. 

A typical workflow of your program using this package will be:
1. Get your `DaosSession` using `DaosSession.getInstance()`. This class has no corresponding object in native DAOS. It is a singleton which helps you to share handles among threads. 
2. Use `session.getPool` and `session.createAndGetPool` to connect to your pool. Specify `PoolMode` to connect it as readonly, read-write or exclusively. 
3. (DFS) Specify a path and pool to mount a Daos File System using `DaosFS.getInstance`. You can also mount it by specifying a container, which you can open as in next step.
4. Use `pool.getContainer` to open a container. The UUID you specify does not need to already exist in the pool. If it does not exist yet, it'll create it for you. Specify `ContainerMode` to connect it as readonly or read-write. 
5. (Object) Use `container.getObject` to open an object. You need to specify a user id, an object class and object features. They will together determine the object layout. Specify `ObjectMode` to connect it as readonly, read-write or exclusively. You can also specify sequential or random I/O. Check [here](https://github.com/daos-stack/daos/tree/master/src/object) for more details.
6. Here starts your actual application:
    * (Object) Use functions in `DaosObject` to perform I/O tasks. Async APIs are still under development. Fetch APIs will return the record size. Notice that DAOS now requires an object has only either array or single value. This is determined upon first write operation. Having both array and single value in an object will yield undefined results. <br/> Use `ByteBuffer.allocateDirect` to get direct byte buffer for I/O tasks. 
    * (DFS) Use `DaosFS.getFile` or `DaosFS.createFile` to open a file in DFS. Use functions in `DaosFile` to perform I/O tasks. The `read` and `write` function returns the size it reads/writes into the buffer. Use `ByteBuffer.allocateDirect` to get direct byte buffer for I/O tasks.<br/>
    Use `DaosFS.getDir` or `DaosFS.createDir` to open a directory in DFS. You can move, rename, remove and list contents under it. Above operations are also available in `DaosFS` in case you do not want to open the directory in Java. But opening it will be slightly more efficient if you need to complete multiple operations with it.<br/>
    You need to open files and directories in each thread in multi-thread applications. They are not shared.
 7. After your application:
    * (DFS) You should call `close` on all of your opened `DaosFile` and `DaosDirectory` in each thread because they are not shared. You  should also call `DaosFS.unmount` after you have done all work with it. It is user's responsibility to ensure all operations have finished before this method is called.  Of course, further function call will probably throw a `DaosNativeException`. 
    * You are **NOT required** to call `close` or `disconnect` on your `DaosPool`, `DaosContainer` and `DaosObject`. You can call it if you know you won't use it any more. Similar to `DaosFS`, You should call `DaosSession.close` after all work is done. Notice that closing `DaosSession` will disconnect all `DaosPool` in it, which will then in turn close all `DaosContainer`, effectively closing all daos instances.
    * If user fails or forgets to close, shutdown hook will be triggered when JVM is about to shutdown. However, if user close `DaosFS` before they close `DaosFile` and `DaosDirectory` or they never close the latter one, the shutdown hooks may not work properly.
    
`DaosNativeException` may be thrown in some other cases. This usually means a daos native call fails, and will contain an error code in the message. Other situation may also trigger it, such as out of memory.

### TODO
1. update DAOS for object erasure code (TBD)
2. Use daos addon array instead(TBD)
3. implement transaction