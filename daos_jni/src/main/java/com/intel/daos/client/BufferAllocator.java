package com.intel.daos.client;

import java.nio.ByteBuffer;

public class BufferAllocator {

  public static ByteBuffer directBuffer(int size){
    return ByteBuffer.allocateDirect(size);
  }
}
