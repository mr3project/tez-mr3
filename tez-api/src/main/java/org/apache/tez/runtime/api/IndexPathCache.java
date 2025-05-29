package org.apache.tez.runtime.api;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.fs.Path;

import javax.annotation.Nullable;

/**
 * Cache for storing mappings between map IDs and their corresponding
 * map output file names and ByteBuffer instances.
 */
public class IndexPathCache {

  /**
   * Inner class to represent a pair of mapOutputFileName and ByteBuffer
   */
  public static class MapOutputInfo {
    private final Path mapOutputFilePath;
    private final ByteBuffer spillRecord;

    public MapOutputInfo(Path mapOutputFilePath, ByteBuffer spillRecord) {
      this.mapOutputFilePath = mapOutputFilePath;
      this.spillRecord = spillRecord;
    }

    public Path getMapOutputFilePath() {
      return mapOutputFilePath;
    }

    public ByteBuffer getSpillRecord() {
      return spillRecord;
    }
  }

  private final ConcurrentHashMap<String, MapOutputInfo> cache;

  public IndexPathCache() {
    this.cache = new ConcurrentHashMap<>();
  }

  public void add(String mapId, Path mapOutputFilePath, ByteBuffer spillRecord) {
    cache.put(mapId, new MapOutputInfo(mapOutputFilePath, spillRecord));
  }

  /**
   * Removes all mappings whose key matches the given prefix
   *
   * @param mapIdPrefix the prefix to match against map IDs
   */
  // synchronized is unnecessary for two reasons:
  //   1. remove() is called by a single thread (the main ShuffleHandlerDaemonProcessor's handleEvents())
  //   2. any element removed here is never added again to cache[]
  //    because it corresponds to a TaskAttempt belonging to a DAG that is already finished.
  public int remove(String mapIdPrefix) {
    Iterator<Map.Entry<String, MapOutputInfo>> iterator = cache.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<String, MapOutputInfo> entry = iterator.next();
      if (entry.getKey().contains(mapIdPrefix)) {
        iterator.remove();
      }
    }
    return cache.size();
  }

  /**
   * Returns the MapOutputInfo (pair of mapOutputFileName and ByteBuffer)
   * for the given mapId
   *
   * @param mapId the map ID to look up
   * @return MapOutputInfo containing mapOutputFileName and ByteBuffer, or null if not found
   */
  public MapOutputInfo get(String mapId) {
    return cache.get(mapId);
  }
}