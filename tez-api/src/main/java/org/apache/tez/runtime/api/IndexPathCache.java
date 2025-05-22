package org.apache.tez.runtime.api;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Cache for storing mappings between map IDs and their corresponding
 * map output file names and ByteBuffer instances.
 */
public class IndexPathCache {

    /**
     * Inner class to represent a pair of mapOutputFileName and ByteBuffer
     */
    public static class MapOutputInfo {
        private final String mapOutputFilePath;
        private final ByteBuffer spillRecord;

        public MapOutputInfo(String mapOutputFileName, ByteBuffer spillRecord) {
            this.mapOutputFilePath = mapOutputFileName;
            this.spillRecord = spillRecord;
        }

        public String getMapOutputFilePath() {
            return mapOutputFilePath;
        }

        public ByteBuffer getSpillRecord() {
            return spillRecord;
        }
    }

    private final ConcurrentHashMap<String, MapOutputInfo> cache;

    /**
     * Constructor initializes the cache
     */
    public IndexPathCache() {
        this.cache = new ConcurrentHashMap<>();
    }

    /**
     * Adds a new mapping from mapId to (mapOutputFileName, ByteBuffer)
     *
     * @param mapId the map ID
     * @param mapOutputFileName the map output file name
     * @param spillRecord the ByteBuffer instance
     */
    public void add(String mapId, String mapOutputFileName, ByteBuffer spillRecord) {
        cache.put(mapId, new MapOutputInfo(mapOutputFileName, spillRecord));
    }

    /**
     * Removes all mappings whose key matches the given prefix
     *
     * @param mapIdPrefix the prefix to match against map IDs
     */
    // synchronized is unnecessary for two reasons:
    //   1. remove() is called by a single thread (the main ShuffleHandlerDaemonProcessor's handleEvents())
    //   2. any element removed here is never added again to cache[]
    //      because it corresponds to a TaskAttempt belonging to a DAG that is already finished.
    public void remove(String mapIdPrefix) {
        Iterator<Map.Entry<String, MapOutputInfo>> iterator = cache.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, MapOutputInfo> entry = iterator.next();
            if (entry.getKey().startsWith(mapIdPrefix)) {
                iterator.remove();
            }
        }
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