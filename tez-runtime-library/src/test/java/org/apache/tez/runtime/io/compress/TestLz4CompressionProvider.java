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
package org.apache.tez.runtime.io.compress;

import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.tez.runtime.api.CompressionProvider;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.junit.Test;

public class TestLz4CompressionProvider {

  @Test
  public void testResolverHonorsHighCompressionSetting() throws Exception {
    Configuration conf = new Configuration(false);
    conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS, true);
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS_CODEC,
        "org.apache.hadoop.io.compress.Lz4Codec");
    conf.setBoolean(CommonConfigurationKeys.IO_COMPRESSION_CODEC_LZ4_USELZ4HC_KEY, true);

    CompressionProvider provider = CompressionResolver.createProvider(conf);
    assertTrue(provider instanceof Lz4CompressionProvider);

    Lz4JniCompressor compressor = (Lz4JniCompressor) provider.createCompressor();
    try {
      Field field = Lz4JniCompressor.class.getDeclaredField("compressor");
      field.setAccessible(true);
      LZ4Compressor delegate = (LZ4Compressor) field.get(compressor);
      assertTrue(LZ4Factory.nativeInstance().highCompressor().getClass().isInstance(delegate));
    } finally {
      compressor.close();
    }
  }
}
