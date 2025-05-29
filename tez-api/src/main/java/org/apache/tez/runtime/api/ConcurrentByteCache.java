package org.apache.tez.runtime.api;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.Iterator;

public class ConcurrentByteCache {

  private final Map<String, MultiByteArrayOutputStream> cacheMap;
  private final Queue<String> insertionOrderQueue; // Tracks keys by insertion order

  public ConcurrentByteCache() {
    this.cacheMap = new ConcurrentHashMap<>();
    this.insertionOrderQueue = new ConcurrentLinkedQueue<>();
  }

  /**
   * Adds a new entry to the cache.
   * This method is called by writer threads. It's assumed that keys are unique
   * and never added twice, so no check for pre-existing keys is performed.
   */
  public void add(String key, MultiByteArrayOutputStream data) {
    // Store the data in the main map
    cacheMap.put(key, data);
    // Add the key to the queue to maintain insertion order
    insertionOrderQueue.offer(key);
  }

  /**
   * Retrieves an entry from the cache.
   * This method is called by reader threads.
   */
  public MultiByteArrayOutputStream get(String key) {
    return cacheMap.get(key);
  }

  public int flushOldestEntries(int count) {
    if (count <= 0) {
      return 0;
    }
    int removedCount = 0;
    for (int i = 0; i < count; i++) {
      String oldestKey = insertionOrderQueue.poll(); // Retrieves and removes the head of the queue
      if (oldestKey == null) {
        break; // Queue is empty, no more entries to remove
      }
      MultiByteArrayOutputStream output = cacheMap.get(oldestKey);  // do not remove
      if (output != null) {
        output.writeBuffersToDisk();
        removedCount++;
      }
    }
    return removedCount;
  }

  public int flushOldestEntriesByteSize(long byteThreshold) {
    if (byteThreshold <= 0) {
      return 0;
    }

    long freedBytes = 0;
    int removedCount = 0;

    // Keep polling oldest keys until we've freed enough bytes
    while (freedBytes < byteThreshold) {
      String oldestKey = insertionOrderQueue.poll();
      if (oldestKey == null) {
        // queue exhausted
        break;
      }

      MultiByteArrayOutputStream output = cacheMap.get(oldestKey);  // do not remove
      if (output != null) {
        output.writeBuffersToDisk();
        freedBytes += output.bufferSize();
        removedCount++;
      }
    }

    return removedCount;
  }

  /**
   * This method is called by the single remover thread.
   * Keys removed from the cache map are not immediately removed from the
   * insertion order queue by this method due to performance reasons; they
   * become "stale" entries in the queue.
   */
  public int removeEntriesByPrefix(String mapIdPrefix) {
    Iterator<Map.Entry<String, MultiByteArrayOutputStream>> iterator = cacheMap.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<String, MultiByteArrayOutputStream> kv = iterator.next();
      String key = kv.getKey();
      if (key.contains(mapIdPrefix)) {
        MultiByteArrayOutputStream value = kv.getValue();
        value.clean();
        iterator.remove(); // Removes the entry from cacheMap
      }
    }
    return cacheMap.size();
  }

  /**
   * Cleans the head of the insertion order queue by removing any keys
   * that no longer have corresponding entries in the main cache map.
   * This process continues until the queue is empty or the key at the
   * head of the queue is confirmed to be present in the cache map.
   *
   * After this method is called, the insertionOrderQueue is either empty,
   * or its first key is guaranteed to have its corresponding data in the cache at
   * the moment of checking (subsequent concurrent removals by other operations excepted).
   *
   * This method is intended to be called by the single remover thread
   * for queue hygiene.
   *
   * @return The number of stale keys removed from the head of the queue.
   */
  public int trimStaleKeysFromQueueHead() {
    int removedStaleCount = 0;
    while (true) {
      String headKey = insertionOrderQueue.peek(); // Look at the head element O(1)

      if (headKey == null) {
        // Queue is empty, nothing more to do.
        break;
      }

      // Check if the key still exists in the main cache map
      if (!cacheMap.containsKey(headKey)) { // O(1) average for ConcurrentHashMap
        // Key is stale (not in cacheMap anymore), remove it from the queue
        insertionOrderQueue.poll(); // Actually remove the stale headKey from queue O(1)
        removedStaleCount++;
      } else {
        // The key at the head of the queue has a corresponding entry in the cache.
        // The condition is met: the head of the queue is a live entry.
        break;
      }
    }
    return removedStaleCount;
  }

  /**
   * Returns the current number of entries in the cache (i.e., in the cacheMap).
   *
   * @return The current size of the cache.
   */
  public int size() {
    return cacheMap.size();
  }

  /**
   * Clears all entries from the cache map and the insertion order queue.
   */
  public void clear() {
    cacheMap.clear();
    insertionOrderQueue.clear();
  }

  /**
   * Peeks at the head of the insertion order queue without removing it.
   * Useful for the remover thread to inspect the oldest key.
   *
   * @return The key at the head of the insertion order queue, or null if the queue is empty.
   */
  public String peekFirstInInsertionOrder() {
    return insertionOrderQueue.peek();
  }

  /**
   * Checks if the cache map contains a specific key.
   * Useful for the remover thread in conjunction with peekFirstInInsertionOrder().
   *
   * @param key The key to check.
   * @return true if the cache map contains the key, false otherwise.
   */
  public boolean containsKey(String key) {
    return cacheMap.containsKey(key);
  }
}
