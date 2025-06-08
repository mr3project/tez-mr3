package org.apache.tez.runtime.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.tez.common.security.JobTokenSecretManager;
import org.apache.tez.http.HttpConnectionParams;

// parameters required by Fetchers
public class FetcherConfig {
  public final Configuration codecConf;
  public final boolean ifileReadAhead;
  public final int ifileReadAheadLength;
  public final JobTokenSecretManager jobTokenSecretMgr;
  public final HttpConnectionParams httpConnectionParams;
  public final RawLocalFileSystem localFs;
  public final LocalDirAllocator localDirAllocator;
  public final String localHostName;
  public final boolean localDiskFetchEnabled;
  public final boolean localDiskFetchOrderedEnabled;
  public final boolean verifyDiskChecksum;

  // read by LogicalInput as well to ensure consistency in shuffling (tez_shuffle vs mapreduce_shuffle)
  public final String auxiliaryService;
  public final boolean compositeFetch;

  public final boolean connectionFailAllInput;
  public final long speculativeExecutionWaitMillis;
  public final int stuckFetcherThresholdMillis;
  public final int stuckFetcherReleaseMillis;

  public FetcherConfig(
    Configuration codecConf,
    boolean ifileReadAhead,
    int ifileReadAheadLength,
    JobTokenSecretManager jobTokenSecretMgr,
    HttpConnectionParams httpConnectionParams,
    RawLocalFileSystem localFs,
    LocalDirAllocator localDirAllocator,
    String localHostName,
    boolean localDiskFetchEnabled,
    boolean localDiskFetchOrderedEnabled,
    boolean verifyDiskChecksum,
    String auxiliaryService,
    boolean compositeFetch,
    boolean connectionFailAllInput,
    long speculativeExecutionWaitMillis,
    int stuckFetcherThresholdMillis,
    int stuckFetcherReleaseMillis) {
    this.codecConf = codecConf;
    this.ifileReadAhead = ifileReadAhead;
    this.ifileReadAheadLength = ifileReadAheadLength;
    this.jobTokenSecretMgr = jobTokenSecretMgr;
    this.httpConnectionParams = httpConnectionParams;
    this.localFs = localFs;
    this.localDirAllocator = localDirAllocator;
    this.localHostName = localHostName;
    this.localDiskFetchEnabled = localDiskFetchEnabled;
    this.localDiskFetchOrderedEnabled = localDiskFetchOrderedEnabled;
    this.verifyDiskChecksum = verifyDiskChecksum;

    this.auxiliaryService = auxiliaryService;
    this.compositeFetch = compositeFetch;

    this.connectionFailAllInput = connectionFailAllInput;
    this.speculativeExecutionWaitMillis = speculativeExecutionWaitMillis;
    this.stuckFetcherThresholdMillis = stuckFetcherThresholdMillis;
    this.stuckFetcherReleaseMillis = stuckFetcherReleaseMillis;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("[ifileReadAhead=");
    sb.append(ifileReadAhead);
    sb.append(", ifileReadAheadLength=");
    sb.append(ifileReadAheadLength);
    sb.append(", httpConnectionParams=");
    sb.append(httpConnectionParams);
    sb.append(", localDiskFetchEnabled=");
    sb.append(localDiskFetchEnabled);
    sb.append(", localDiskFetchOrderedEnabled=");
    sb.append(localDiskFetchOrderedEnabled);
    sb.append(", compositeFetch=");
    sb.append(compositeFetch);
    sb.append(", speculativeExecutionWaitMillis=");
    sb.append(speculativeExecutionWaitMillis);
    sb.append(", stuckFetcherThresholdMillis=");
    sb.append(stuckFetcherThresholdMillis);
    sb.append(", stuckFetcherReleaseMillis=");
    sb.append(stuckFetcherReleaseMillis);
    sb.append("]");
    return sb.toString();
  }
}

