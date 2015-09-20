package com.asiainfo.ocdp.stream.manager

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

/**
 * Created by leo on 8/26/15.
 */
class ContextWaiter {

  private val lock = new ReentrantLock()
  private val condition = lock.newCondition()

  // Guarded by "lock"
  private var error: Throwable = null

  // Guarded by "lock"
  private var stopped: Boolean = false

  def notifyError(e: Throwable): Unit = {
    lock.lock()
    try {
      error = e
      condition.signalAll()
    } finally {
      lock.unlock()
    }
  }

  def notifyStop(): Unit = {
    lock.lock()
    try {
      stopped = true
      condition.signalAll()
    } finally {
      lock.unlock()
    }
  }

  /**
   * Return `true` if it's stopped; or throw the reported error if `notifyError` has been called; or
   * `false` if the waiting time detectably elapsed before return from the method.
   */
  def waitForStopOrError(timeout: Long = -1): Boolean = {
    lock.lock()
    try {
      if (timeout < 0) {
        while (!stopped && error == null) {
          condition.await()
        }
      } else {
        var nanos = TimeUnit.MILLISECONDS.toNanos(timeout)
        while (!stopped && error == null && nanos > 0) {
          nanos = condition.awaitNanos(nanos)
        }
      }
      // If already had error, then throw it
      if (error != null) throw error
      // already stopped or timeout
      stopped
    } finally {
      lock.unlock()
    }
  }

}
