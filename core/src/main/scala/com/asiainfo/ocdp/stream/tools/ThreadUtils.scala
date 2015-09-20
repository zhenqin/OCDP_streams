package com.asiainfo.ocdp.stream.tools

import java.util.concurrent.{ThreadFactory, SynchronousQueue, TimeUnit, ThreadPoolExecutor}

import com.google.common.util.concurrent.ThreadFactoryBuilder

/**
 * Created by tsingfu on 15/8/24.
 */
object ThreadUtils {

  /**
   * Create a cached thread pool whose max number of threads is `maxThreadNumber`. Thread names
   * are formatted as prefix-ID, where ID is a unique, sequentially assigned integer.
   */
  def newDaemonCachedThreadPool(prefix: String, maxThreadNumber: Int): ThreadPoolExecutor = {
    val threadFactory = namedThreadFactory(prefix)
    new ThreadPoolExecutor(
      0, maxThreadNumber, 60L, TimeUnit.SECONDS, new SynchronousQueue[Runnable], threadFactory)
  }

  /**
   * Create a thread factory that names threads with a prefix and also sets the threads to daemon.
   */
  def namedThreadFactory(prefix: String): ThreadFactory = {
    new ThreadFactoryBuilder().setDaemon(true).setNameFormat(prefix + "-%d").build()
  }
}
