package com.intel.daos.client;

import java.io.IOException;

public class DaosIOException extends IOException {

  private int errorCode;

  public DaosIOException(String msg){
    super(msg);
  }

  public DaosIOException(Throwable cause){
    super(cause);
  }

  public DaosIOException(String msg, int errorCode){
    super(msg);
    this.errorCode = errorCode;
  }

  public DaosIOException(String msg, int errorCode, Throwable cause){
    super(msg, cause);
    this.errorCode = errorCode;
  }

  public String toString(){
    //TODO: parse error code if errorcode > 0
    return null;
  }
}
