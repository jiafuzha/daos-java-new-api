package com.intel.daos.client;

import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;

public final class ShutdownHookManager {

  private static final Deque<Runnable> hookStack = new ConcurrentLinkedDeque<>();

  static{
    Runtime.getRuntime().addShutdownHook(new Thread( () -> {
      Runnable hook;
      while ((hook = hookStack.pollLast()) != null) {
        hook.run();
      }
    }));
  }

  public static void addHook(Runnable runnable){
    hookStack.add(runnable);
  }
}
