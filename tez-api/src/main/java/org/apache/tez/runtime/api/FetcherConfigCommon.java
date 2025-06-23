package org.apache.tez.runtime.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.tez.common.security.JobTokenSecretManager;
import org.apache.tez.http.HttpConnectionParams;

// parameters common to Fetchers
public class FetcherConfigCommon {

  public final Configuration codecConf;
  public final JobTokenSecretManager jobTokenSecretMgr;
  public final HttpConnectionParams httpConnectionParams;

  public final RawLocalFileSystem localFs;
  public final LocalDirAllocator localDirAllocator;
  public final String localHostName;
  public final boolean localDiskFetchEnabled;
  public final boolean localDiskFetchOrderedEnabled;
  public final boolean verifyDiskChecksum;
  public final boolean compositeFetch;
  public final boolean connectionFailAllInput;

  public FetcherConfigCommon(
      Configuration codecConf,
      JobTokenSecretManager jobTokenSecretMgr,
      HttpConnectionParams httpConnectionParams,
      RawLocalFileSystem localFs,
      LocalDirAllocator localDirAllocator,
      String localHostName,
      boolean localDiskFetchEnabled,
      boolean localDiskFetchOrderedEnabled,
      boolean verifyDiskChecksum,
      boolean compositeFetch,
      boolean connectionFailAllInput) {
    this.codecConf = codecConf;
    this.jobTokenSecretMgr = jobTokenSecretMgr;
    this.httpConnectionParams = httpConnectionParams;

    this.localFs = localFs;
    this.localDirAllocator = localDirAllocator;
    this.localHostName = localHostName;
    this.localDiskFetchEnabled = localDiskFetchEnabled;
    this.localDiskFetchOrderedEnabled = localDiskFetchOrderedEnabled;
    this.verifyDiskChecksum = verifyDiskChecksum;
    this.compositeFetch = compositeFetch;
    this.connectionFailAllInput = connectionFailAllInput;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(", httpConnectionParams=");
    sb.append(httpConnectionParams);
    sb.append(", localDiskFetchEnabled=");
    sb.append(localDiskFetchEnabled);
    sb.append(", localDiskFetchOrderedEnabled=");
    sb.append(localDiskFetchOrderedEnabled);
    sb.append(", compositeFetch=");
    sb.append(compositeFetch);
    sb.append("]");
    return sb.toString();
  }
}
