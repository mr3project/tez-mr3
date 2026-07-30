/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.tez.runtime.library.common.shuffle.orderedgrouped;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

abstract class MergeThread<T> extends Thread {
  
  private static final Logger LOG = LoggerFactory.getLogger(MergeThread.class);

  private volatile boolean inProgress = false;
  private final List<T> inputs = new ArrayList<T>();
  protected final MergeManager manager;
  private final ExceptionReporter reporter;
  private boolean closed = false;
  private final int mergeFactor;

  private Thread shuffleSchedulerThread;
  
  public MergeThread(MergeManager manager, int mergeFactor,
                     ExceptionReporter reporter) {
    this.manager = manager;
    this.mergeFactor = mergeFactor;
    this.reporter = reporter;
  }
  
  public synchronized void close() throws InterruptedException {
    closed = true;
    if (!Thread.currentThread().isInterrupted()) {
      waitForMerge();
    }
    interrupt();
  }

  public void setParentThread(Thread shuffleSchedulerThread) {
    this.shuffleSchedulerThread = shuffleSchedulerThread;
  }

  // always called inside synchronized (Merger) {}
  public boolean isInProgress() {
    return inProgress;
  }
  
  public synchronized void startMerge(Set<T> inputs) {
    if (!closed) {
      this.inputs.clear();
      inProgress = true;
      Iterator<T> iter=inputs.iterator();
      for (int ctr = 0; iter.hasNext() && ctr < mergeFactor; ++ctr) {
        this.inputs.add(iter.next());
        iter.remove();
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("{}: Starting merge with {} segments, while ignoring {} segments",
            getName(), this.inputs.size(), inputs.size());
      }
      notifyAll();
    }
  }

  public synchronized void waitForMerge() throws InterruptedException {
    while (inProgress) {
      if (shuffleSchedulerThread != null && !shuffleSchedulerThread.isAlive()) {
        return;
      }
      wait(1000);
    }
  }

  protected abstract Map<String, String> getMdcContext();

  public void run() {
    Map<String, String> oldMdcContext = MDC.getCopyOfContextMap();
    try {
      ShuffleUtils.restoreMdc(getMdcContext());
      while (true) {
        try {
          // Wait for notification to start the merge...
          synchronized (this) {
            while (!inProgress) {
              if (shuffleSchedulerThread != null&& !shuffleSchedulerThread.isAlive()) {
                return;
              }
              wait(1000);
            }
          }

          // Merge
          merge(inputs);
        } catch (InterruptedException ie) {
          // Meant to handle a shutdown of the entire fetch/merge process
          Thread.currentThread().interrupt();
          return;
        } catch(Throwable t) {
          reporter.reportException(t);
          return;
        } finally {
          synchronized (this) {
            // Clear inputs
            inputs.clear();
            inProgress = false;
            notifyAll();
          }
        }
      }
    } finally {
      ShuffleUtils.restoreMdc(oldMdcContext);
    }
  }

  public abstract void merge(List<T> inputs) 
      throws IOException, InterruptedException;
}
