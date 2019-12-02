package com.intel.daos.client;

public final class DaosUtils {

  private DaosUtils(){}

  public static String normalize(String path){
    if(path == null || (path=path.trim()).length() == 0){
      return "";
    }
    //TODO: normalize
    return path;
  }

  /**
   * split parent and name
   * @param path
   * @return
   */
  public static String[] parsePath(String path) {
    int slash = path.lastIndexOf('/');
    if(slash >= 0){
      return new String[] {path.substring(0, slash), path.substring(slash+1)};
    }
    return new String[] {path};
  }
}
