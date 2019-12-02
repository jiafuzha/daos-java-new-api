package com.intel.daos.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.security.AccessController;
import java.security.PrivilegedAction;

public class Cleaner extends PhantomReference<Object> {
  private static final ReferenceQueue<Object> dummyQueue = new ReferenceQueue();

  private final Runnable action;

  private Cleaner(Object referent, Runnable action) {
    super(referent, dummyQueue);
    this.action = action;
  }

  public static Cleaner create(Object referent, Runnable action) {
    if(action == null){
      return null;
    }
    return new Cleaner(referent, action);
  }

  public void clean() {
    try {
      this.action.run();
    } catch (final Throwable th) {
      AccessController.doPrivileged(new PrivilegedAction<Void>() {
        public Void run() {
          if (System.err != null) {
            (new Error("Cleaner terminated abnormally", th)).printStackTrace();
          }
          System.exit(1);
          return null;
        }
      });
    }
  }

  public static class CleanerTask implements Runnable{

    private static final Logger log = LoggerFactory.getLogger(CleanerTask.class);

    @Override
    public void run() {
      int count = 0;
      while(!Thread.interrupted()){
        Cleaner cleaner = (Cleaner) dummyQueue.poll();
        if(cleaner == null){
          count++;
          if(count > 16){
            try {
              Thread.sleep(2000);
            }catch (InterruptedException e){
              log.info("cleaner thread interrupted", e);
              break;
            }
          }
          continue;
        }
        count = 0;
        cleaner.clean();
      }
    }
  }
}
