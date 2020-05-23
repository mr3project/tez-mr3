package org.apache.tez.runtime.api;

import org.apache.hadoop.security.UserGroupInformation;

import java.util.concurrent.ExecutorService;

public class ExecutorServiceUserGroupInformation {
  private ExecutorService executorService;
  private UserGroupInformation ugi;

  public ExecutorServiceUserGroupInformation(ExecutorService executorService, UserGroupInformation ugi) {
    this.executorService = executorService;
    this.ugi = ugi;
  }

  public ExecutorService getExecutorService() {
    return executorService;
  }

  public UserGroupInformation getUgi() {
    return ugi;
  }
}
