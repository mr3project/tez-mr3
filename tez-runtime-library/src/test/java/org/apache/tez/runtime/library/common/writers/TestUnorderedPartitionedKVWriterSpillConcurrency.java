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

package org.apache.tez.runtime.library.common.writers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import org.junit.Test;

import sun.misc.Unsafe;

public class TestUnorderedPartitionedKVWriterSpillConcurrency {

  private static final long BLOCK_TIMEOUT_MILLIS = 5000;

  @Test
  public void testScheduleSpillBlockingWaitsForSlotAndPropagatesInterrupt() throws Exception {
    Semaphore availableSlots = new Semaphore(0);
    UnorderedPartitionedKVWriter writer = newWriter(availableSlots, new ReentrantLock(), 1, false);
    AtomicReference<Throwable> failure = new AtomicReference<>();

    Thread scheduler = new Thread(() -> {
      try {
        invokeScheduleSpillBlocking(writer, 1);
        failure.set(new AssertionError("scheduleSpillBlocking returned without a slot"));
      } catch (InterruptedException expected) {
        // Expected: blocking spill scheduling must remain interruptible.
      } catch (Throwable t) {
        failure.set(t);
      }
    });
    scheduler.setDaemon(true);
    scheduler.start();

    waitUntilBlocked(scheduler);
    assertEquals(0, availableSlots.availablePermits());
    scheduler.interrupt();
    scheduler.join(BLOCK_TIMEOUT_MILLIS);

    assertFalse("scheduler did not stop after interruption", scheduler.isAlive());
    assertNull(failure.get());
  }

  @Test
  public void testCloseDoesNotHoldSpillLockWhileWaitingForSlot() throws Exception {
    Semaphore availableSlots = new Semaphore(0);
    ReentrantLock spillLock = new ReentrantLock();
    UnorderedPartitionedKVWriter writer = newWriter(availableSlots, spillLock, 1, true);
    AtomicReference<Throwable> closeResult = new AtomicReference<>();

    Thread closeThread = new Thread(() -> {
      try {
        writer.close();
        closeResult.set(new AssertionError("close returned without a spill slot"));
      } catch (InterruptedException expected) {
        // Expected after proving the callback-side lock remains available.
      } catch (Throwable t) {
        closeResult.set(t);
      }
    });
    closeThread.setDaemon(true);
    closeThread.start();

    waitUntilBlocked(closeThread);
    assertEquals("CLOSED", getField(writer, "writerState").toString());
    assertTrue("spill callback could not acquire spillLock while close waited for a slot",
        spillLock.tryLock(BLOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS));
    spillLock.unlock();

    closeThread.interrupt();
    closeThread.join(BLOCK_TIMEOUT_MILLIS);
    assertFalse("close did not stop after interruption", closeThread.isAlive());
    assertNull(closeResult.get());
  }

  private static void waitUntilBlocked(Thread thread) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(BLOCK_TIMEOUT_MILLIS);
    while (thread.getState() != Thread.State.WAITING && System.nanoTime() < deadline) {
      if (!thread.isAlive()) {
        break;
      }
      Thread.sleep(10);
    }
    assertEquals("thread did not block waiting for a spill slot", Thread.State.WAITING, thread.getState());
  }

  private static void invokeScheduleSpillBlocking(UnorderedPartitionedKVWriter writer, int minimum)
      throws Throwable {
    Method method = UnorderedPartitionedKVWriter.class.getDeclaredMethod("scheduleSpillBlocking", int.class);
    method.setAccessible(true);
    try {
      method.invoke(writer, minimum);
    } catch (InvocationTargetException e) {
      throw e.getCause();
    }
  }

  private static UnorderedPartitionedKVWriter newWriter(Semaphore availableSlots,
      ReentrantLock spillLock, int filledBufferCount, boolean pipelinedShuffle) throws Exception {
    UnorderedPartitionedKVWriter writer = (UnorderedPartitionedKVWriter) getUnsafe()
        .allocateInstance(UnorderedPartitionedKVWriter.class);
    List<Object> filledBuffers = new ArrayList<>();
    for (int i = 0; i < filledBufferCount; i++) {
      filledBuffers.add(null);
    }
    setField(writer, "availableSlots", availableSlots);
    setField(writer, "spillLock", spillLock);
    setField(writer, "spillInProgress", spillLock.newCondition());
    setField(writer, "filledBuffers", filledBuffers);
    setField(writer, "isPipelinedShuffle", pipelinedShuffle);
    setField(writer, "writerState", getRunningWriterState());
    return writer;
  }

  private static void setField(Object target, String name, Object value) throws Exception {
    Field field = UnorderedPartitionedKVWriter.class.getDeclaredField(name);
    field.setAccessible(true);
    field.set(target, value);
  }

  private static Object getField(Object target, String name) throws Exception {
    Field field = UnorderedPartitionedKVWriter.class.getDeclaredField(name);
    field.setAccessible(true);
    return field.get(target);
  }

  private static Object getRunningWriterState() throws Exception {
    Class<?> writerState = Class.forName(UnorderedPartitionedKVWriter.class.getName() + "$WriterState");
    for (Object value : writerState.getEnumConstants()) {
      if (value.toString().equals("RUNNING")) {
        return value;
      }
    }
    throw new AssertionError("RUNNING writer state not found");
  }

  private static Unsafe getUnsafe() throws Exception {
    Field field = Unsafe.class.getDeclaredField("theUnsafe");
    field.setAccessible(true);
    return (Unsafe) field.get(null);
  }
}
