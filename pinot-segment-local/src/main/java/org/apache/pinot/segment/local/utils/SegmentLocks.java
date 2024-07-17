/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.segment.local.utils;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SegmentLocks {
  private final LoadingCache<Pair<String, String>, Lock> _locks =
      CacheBuilder.newBuilder().weakValues().build(new CacheLoader<>() {
        @Override
        public Lock load(Pair<String, String> key) {
          return new TrackableReentrantLock(true);
        }
      });

  private static final Logger LOG = LoggerFactory.getLogger(SegmentLocks.class);
  private static final int SEGMENT_LOCK_WAIT_SECONDS;

  static {
    SEGMENT_LOCK_WAIT_SECONDS = Integer.parseInt(System.getProperty("segmentLockWaitSeconds", "60"));
    LOG.info("segmentLockWaitSeconds: {}", SEGMENT_LOCK_WAIT_SECONDS);
  }

  public Lock getLock(String tableNameWithType, String segmentName) {
    return _locks.getUnchecked(Pair.of(tableNameWithType, segmentName));
  }

  // DO NOT use global lock because that can break tests with multiple server instances

  @Deprecated
  private static final SegmentLocks DEFAULT_LOCKS = create();

  @Deprecated
  public static Lock getSegmentLock(String tableNameWithType, String segmentName) {
    return DEFAULT_LOCKS.getLock(tableNameWithType, segmentName);
  }

  @Deprecated
  public static SegmentLocks create() {
    return new SegmentLocks();
  }

  @Deprecated
  public static SegmentLocks create(int numLocks) {
    return new SegmentLocks();
  }

  private static class TrackableReentrantLock extends ReentrantLock {
    private volatile String _owner;

    public TrackableReentrantLock(boolean fair) {
      super(fair);
    }

    @Override
    public void lock() {
      boolean acquired = tryLockingWithinLimitedTime();
      // Lock only when lock is not acquired in first attempt
      if (!acquired) {
        super.lock();
      }
      _owner = Thread.currentThread().getName();
      LOG.debug("lock acquired. lock: [{}], owner: [{}], holding count: {}", this, _owner, getHoldCount());
    }

    @Override
    public void lockInterruptibly()
        throws InterruptedException {
      boolean acquired = tryLockingWithinLimitedTime();
      if (Thread.interrupted()) {
        throw new InterruptedException("interrupted while trying to acquire lock");
      }
      // Lock only when lock is not acquired in first attempt
      if (!acquired) {
        super.lockInterruptibly();
      }
      _owner = Thread.currentThread().getName();
      LOG.debug("lock acquired. lock: [{}], owner: [{}], holding count: {}", this, _owner, getHoldCount());
    }

    private boolean tryLockingWithinLimitedTime() {
      boolean acquired = false;
      try {
        acquired = tryLock(SEGMENT_LOCK_WAIT_SECONDS, TimeUnit.SECONDS);
        if (!acquired) {
          LOG.warn("failed to acquire in limited time. lock: [{}], owner: [{}], holding count: {}", this, _owner,
              getHoldCount());
        }
      } catch (InterruptedException e) {
        // Need to maintain the same behavior as existing.
        // So, resetting the interrupt flag and propagating to the caller
        Thread.currentThread().interrupt();
      }
      return acquired;
    }

    @Override
    public void unlock() {
      // Handles the cases of nested locking and release in same thread
      if (getHoldCount() == 1 && isHeldByCurrentThread()) {
        _owner = null;
      }
      super.unlock();
      LOG.debug("lock released. lock: [{}], owner: [{}], holding count: {}", this, _owner, getHoldCount());
    }
  }
}
