package org.apache.tez.runtime.api;

// parameters required by Fetchers
public class FetcherConfig {

  public final boolean ifileReadAhead;
  public final int ifileReadAheadLength;
  public final long speculativeExecutionWaitMillis;
  public final int stuckFetcherThresholdMillis;
  public final int stuckFetcherReleaseMillis;
  public final int maxSpeculativeFetchAttempts;

  public FetcherConfig(
      boolean ifileReadAhead,
      int ifileReadAheadLength,
      long speculativeExecutionWaitMillis,
      int stuckFetcherThresholdMillis,
      int stuckFetcherReleaseMillis,
      int maxSpeculativeFetchAttempts) {
    this.ifileReadAhead = ifileReadAhead;
    this.ifileReadAheadLength = ifileReadAheadLength;
    this.speculativeExecutionWaitMillis = speculativeExecutionWaitMillis;
    this.stuckFetcherThresholdMillis = stuckFetcherThresholdMillis;
    this.stuckFetcherReleaseMillis = stuckFetcherReleaseMillis;
    this.maxSpeculativeFetchAttempts = maxSpeculativeFetchAttempts;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("[ifileReadAhead=");
    sb.append(ifileReadAhead);
    sb.append(", ifileReadAheadLength=");
    sb.append(ifileReadAheadLength);
    sb.append(", speculativeExecutionWaitMillis=");
    sb.append(speculativeExecutionWaitMillis);
    sb.append(", stuckFetcherThresholdMillis=");
    sb.append(stuckFetcherThresholdMillis);
    sb.append(", stuckFetcherReleaseMillis=");
    sb.append(stuckFetcherReleaseMillis);
    sb.append(", maxSpeculativeFetchAttempts=");
    sb.append(maxSpeculativeFetchAttempts);
    sb.append("]");
    return sb.toString();
  }
}

