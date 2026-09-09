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

package org.apache.tez.runtime.library.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.io.compress.ZStandardCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.http.HttpConnectionParams;
import org.apache.tez.runtime.api.FetcherConfig;
import org.apache.tez.runtime.api.FetcherConfigCommon;
import org.apache.tez.runtime.api.TaskContext;
import org.apache.tez.runtime.io.compress.CompressionProvider;
import org.apache.tez.runtime.io.compress.CompressionResolver;
import org.apache.tez.runtime.io.compress.Compressor;
import org.apache.tez.runtime.io.compress.Decompressor;
import org.apache.tez.runtime.io.compress.TezCompressionOutputStream;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.ConfigUtils;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;
import org.apache.tez.runtime.library.common.sort.impl.IFileInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datamonad.mr3.common.security.JobTokenSecretManager;

import javax.crypto.SecretKey;

public final class CodecUtils {

  private static final Logger LOG = LoggerFactory.getLogger(CodecUtils.class);
  static final int DEFAULT_BUFFER_SIZE = 256 * 1024;

  private CodecUtils() {
  }

  // conf is specific to each RuntimeTask
  public static FetcherConfigCommon constructFetcherConfigCommon(
      Configuration conf, TaskContext taskContext) throws IOException {
    boolean enabled = ConfigUtils.shouldCompressIntermediateOutput(conf);
    Configuration codecConf = CodecUtils.reduceConfForCodec(conf, enabled);
    Class<? extends CompressionCodec> codecClass = null;
    int bufferSize = -1;
    if (enabled) {
      codecClass = ConfigUtils.getIntermediateOutputCompressorClass(codecConf, DefaultCodec.class);
      if (codecClass == SnappyCodec.class) {
        bufferSize = codecConf.getInt(
            CommonConfigurationKeys.IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_KEY,
            CommonConfigurationKeys.IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_DEFAULT);
      } else if (codecClass == ZStandardCodec.class) {
        bufferSize = codecConf.getInt(
            CommonConfigurationKeys.IO_COMPRESSION_CODEC_ZSTD_BUFFER_SIZE_KEY,
            CommonConfigurationKeys.IO_COMPRESSION_CODEC_ZSTD_BUFFER_SIZE_DEFAULT);
      }
    }

    String auxiliaryService = ShuffleUtils.getTezShuffleHandlerServiceId(conf);
    SecretKey shuffleSecret = ShuffleUtils.getJobTokenSecretFromTokenBytes(
        taskContext.getServiceConsumerMetaData(auxiliaryService));
    JobTokenSecretManager jobTokenSecretMgr = new JobTokenSecretManager(shuffleSecret);

    boolean compositeFetch = ShuffleUtils.isTezShuffleHandler(conf);
    HttpConnectionParams httpConnectionParams = ShuffleUtils.getHttpConnectionParams(conf, compositeFetch);

    RawLocalFileSystem localFs = (RawLocalFileSystem) FileSystem.getLocal(conf).getRaw();
    LocalDirAllocator localDirAllocator = new LocalDirAllocator(TezRuntimeFrameworkConfigs.LOCAL_DIRS);
    String localHostName = taskContext.getExecutionContext().getHostName();

    boolean localDiskFetchEnabled = conf.getBoolean(
        TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH,
        TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH_DEFAULT);
    boolean localDiskFetchOrderedEnabled = conf.getBoolean(
        TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH_ORDERED,
        TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH_ORDERED_DEFAULT);
    boolean verifyDiskChecksum = conf.getBoolean(
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_VERIFY_DISK_CHECKSUM,
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_VERIFY_DISK_CHECKSUM_DEFAULT);
    boolean connectionFailAllInput = conf.getBoolean(
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_CONNECTION_FAIL_ALL_INPUT,
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_CONNECTION_FAIL_ALL_INPUT_DEFAULT);

    return new FetcherConfigCommon(
        codecConf,
        codecClass,
        bufferSize,
        jobTokenSecretMgr,
        httpConnectionParams,
        localFs,
        localDirAllocator,
        localHostName,
        localDiskFetchEnabled,
        localDiskFetchOrderedEnabled,
        verifyDiskChecksum,
        compositeFetch,
        connectionFailAllInput);
  }

  // conf is specific to each RuntimeTask, called from TezContainerWorkerEnv
  public static FetcherConfig constructFetcherConfig(Configuration conf) {
    boolean ifileReadAhead = conf.getBoolean(
        TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD,
        TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_DEFAULT);
    int ifileReadAheadLength = ifileReadAhead ? conf.getInt(
        TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_BYTES,
        TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_BYTES_DEFAULT) : 0;

    long speculativeExecutionWaitMillis = (long)conf.getInt(
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_SPECULATIVE_FETCH_WAIT_MILLIS,
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_SPECULATIVE_FETCH_WAIT_MILLIS_DEFAULT);
    int stuckFetcherThresholdMillis = conf.getInt(
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_STUCK_FETCHER_THRESHOLD_MILLIS,
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_STUCK_FETCHER_THRESHOLD_MILLIS_DEFAULT);
    int stuckFetcherReleaseMillis = conf.getInt(
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_STUCK_FETCHER_RELEASE_MILLIS,
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_STUCK_FETCHER_RELEASE_MILLIS_DEFAULT);
    int maxSpeculativeFetchAttempts = conf.getInt(
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MAX_SPECULATIVE_FETCH_ATTEMPTS,
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MAX_SPECULATIVE_FETCH_ATTEMPTS_DEFAULT);

    return new FetcherConfig(
        ifileReadAhead,
        ifileReadAheadLength,
        speculativeExecutionWaitMillis,
        stuckFetcherThresholdMillis,
        stuckFetcherReleaseMillis,
        maxSpeculativeFetchAttempts);
  }

  private static Configuration reduceConfForCodec(Configuration conf, boolean enabled) {
    Configuration newConf = new Configuration(false);

    newConf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS, enabled);

    String compressionCodec = conf.get(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS_CODEC);
    if (enabled && compressionCodec != null) {
      newConf.set(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS_CODEC, compressionCodec);
    }

    for (Map.Entry<String, String> entry: conf) {
      if (entry.getKey().startsWith("io.")) {
        newConf.set(entry.getKey(), entry.getValue());
      }
    }

    return newConf;
  }

  public static CompressionCodec getCodec(Configuration codecConf,
      Class<? extends CompressionCodec> codecClass, int bufferSize) throws IOException {
    if (codecClass == null) {
      return null;
    }

    CompressionCodec codec = ReflectionUtils.newInstance(codecClass, codecConf);

    // Resolve eagerly using only the configured codec class name. In particular, do not call
    // getCompressorType(), since Hadoop codecs may perform native-library checks there.
    CompressionResolver.getProvider(codec);
    return codec;
  }

  public static TezCompressionOutputStream createOutputStream(CompressionCodec codec,
      OutputStream checksumOut, Compressor compressor) throws IOException {
    CompressionProvider provider = CompressionResolver.getProvider(codec);
    return provider.createOutputStream(
        checksumOut, compressor, CompressionResolver.DEFAULT_BUFFER_SIZE);
  }

  public static InputStream getDecompressedInputStreamWithBufferSize(CompressionCodec codec,
      IFileInputStream checksumIn, Decompressor decompressor, int compressedLength)
      throws IOException {
    CompressionProvider provider = CompressionResolver.getProvider(codec);
    return provider.createInputStream(
        checksumIn, decompressor, CompressionResolver.DEFAULT_BUFFER_SIZE);
  }

  public static String getBufferSizeProperty(CompressionCodec codec) {
    String codecClassName = codec.getClass().getName();
    switch (codecClassName) {
      case "org.apache.hadoop.io.compress.SnappyCodec":
        return CommonConfigurationKeys.IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_KEY;
      case "org.apache.hadoop.io.compress.ZStandardCodec":
        return CommonConfigurationKeys.IO_COMPRESSION_CODEC_ZSTD_BUFFER_SIZE_KEY;
      case "org.apache.hadoop.io.compress.DefaultCodec":
      case "org.apache.hadoop.io.compress.BZip2Codec":
      case "org.apache.hadoop.io.compress.GzipCodec":
        return CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY;
      case "org.apache.hadoop.io.compress.LzoCodec":
      case "com.hadoop.compression.lzo.LzoCodec":
        return CommonConfigurationKeys.IO_COMPRESSION_CODEC_LZO_BUFFERSIZE_KEY;
      case "org.apache.hadoop.io.compress.Lz4Codec":
        return CommonConfigurationKeys.IO_COMPRESSION_CODEC_LZ4_BUFFERSIZE_KEY;
      default:
        return null;
    }
  }

  public static int getDefaultBufferSize(CompressionCodec codec) {
    return getDefaultBufferSize(codec.getClass().getName());
  }

  public static int getDefaultBufferSize(String codecClassName) {
    switch (codecClassName) {
    case "org.apache.hadoop.io.compress.SnappyCodec":
      return CommonConfigurationKeys.IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_DEFAULT;
    case "org.apache.hadoop.io.compress.ZStandardCodec":
      return CommonConfigurationKeys.IO_COMPRESSION_CODEC_ZSTD_BUFFER_SIZE_DEFAULT;
    case "org.apache.hadoop.io.compress.DefaultCodec":
    case "org.apache.hadoop.io.compress.BZip2Codec":
    case "org.apache.hadoop.io.compress.GzipCodec":
      return CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT;
    case "org.apache.hadoop.io.compress.LzoCodec":
    case "com.hadoop.compression.lzo.LzoCodec":
      return CommonConfigurationKeys.IO_COMPRESSION_CODEC_LZO_BUFFERSIZE_DEFAULT;
    case "org.apache.hadoop.io.compress.Lz4Codec":
      return CommonConfigurationKeys.IO_COMPRESSION_CODEC_LZ4_BUFFERSIZE_DEFAULT;
    default:
      return DEFAULT_BUFFER_SIZE;
    }
  }
}
