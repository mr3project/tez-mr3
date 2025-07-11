/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.serviceplugins.api;

import java.util.Arrays;

import org.apache.tez.common.Preconditions;

/**
 * An {@link ServicePluginsDescriptor} describes the list of plugins running within the AM for
 * sourcing resources, launching and executing work.
 */
public class ServicePluginsDescriptor {

  private final boolean enableContainers;
  private final boolean enableUber;

  private TaskSchedulerDescriptor[] taskSchedulerDescriptors;
  private ContainerLauncherDescriptor[] containerLauncherDescriptors;
  private TaskCommunicatorDescriptor[] taskCommunicatorDescriptors;

  private ServicePluginsDescriptor(boolean enableContainers, boolean enableUber,
                                   TaskSchedulerDescriptor[] taskSchedulerDescriptors,
                                   ContainerLauncherDescriptor[] containerLauncherDescriptors,
                                   TaskCommunicatorDescriptor[] taskCommunicatorDescriptors) {
    this.enableContainers = enableContainers;
    this.enableUber = enableUber;
    Preconditions.checkArgument(taskSchedulerDescriptors == null || taskSchedulerDescriptors.length > 0,
        "TaskSchedulerDescriptors should either not be specified or at least 1 should be provided");
    this.taskSchedulerDescriptors = taskSchedulerDescriptors;
    Preconditions.checkArgument(containerLauncherDescriptors == null || containerLauncherDescriptors.length > 0,
        "ContainerLauncherDescriptor should either not be specified or at least 1 should be provided");
    this.containerLauncherDescriptors = containerLauncherDescriptors;
    Preconditions.checkArgument(taskCommunicatorDescriptors == null || taskCommunicatorDescriptors.length > 0,
        "TaskCommunicatorDescriptors should either not be specified or at least 1 should be provided");
    this.taskCommunicatorDescriptors = taskCommunicatorDescriptors;
  }

  /**
   * Create a service plugin descriptor with the provided plugins. Regular containers will also be enabled
   * when using this method.
   *
   * @param taskSchedulerDescriptor the task scheduler plugin descriptors
   * @param containerLauncherDescriptors the container launcher plugin descriptors
   * @param taskCommunicatorDescriptors the task communicator plugin descriptors
   * @return a {@link ServicePluginsDescriptor} instance
   */
  public static ServicePluginsDescriptor create(TaskSchedulerDescriptor[] taskSchedulerDescriptor,
                                                ContainerLauncherDescriptor[] containerLauncherDescriptors,
                                                TaskCommunicatorDescriptor[] taskCommunicatorDescriptors) {
    return new ServicePluginsDescriptor(true, false, taskSchedulerDescriptor,
        containerLauncherDescriptors, taskCommunicatorDescriptors);
  }

  /**
   * Create a service plugin descriptor with the provided plugins. Also allows specification of whether
   * in-AM execution is enabled. Container execution is enabled by default.
   *
   * @param enableUber whether to enable execution in the AM or not
   * @param taskSchedulerDescriptor the task scheduler plugin descriptors
   * @param containerLauncherDescriptors the container launcher plugin descriptors
   * @param taskCommunicatorDescriptors the task communicator plugin descriptors
   * @return a {@link ServicePluginsDescriptor} instance
   */
  public static ServicePluginsDescriptor create(boolean enableUber,
                                                TaskSchedulerDescriptor[] taskSchedulerDescriptor,
                                                ContainerLauncherDescriptor[] containerLauncherDescriptors,
                                                TaskCommunicatorDescriptor[] taskCommunicatorDescriptors) {
    return new ServicePluginsDescriptor(true, enableUber, taskSchedulerDescriptor,
        containerLauncherDescriptors, taskCommunicatorDescriptors);
  }

  /**
   * Create a service plugin descriptor with the provided plugins. Also allows specification of whether
   * container execution and in-AM execution will be enabled.
   *
   * @param enableContainers whether to enable execution in containers
   * @param enableUber whether to enable execution in the AM or not
   * @param taskSchedulerDescriptor the task scheduler plugin descriptors
   * @param containerLauncherDescriptors the container launcher plugin descriptors
   * @param taskCommunicatorDescriptors the task communicator plugin descriptors
   * @return a {@link ServicePluginsDescriptor} instance
   */
  public static ServicePluginsDescriptor create(boolean enableContainers, boolean enableUber,
                                                TaskSchedulerDescriptor[] taskSchedulerDescriptor,
                                                ContainerLauncherDescriptor[] containerLauncherDescriptors,
                                                TaskCommunicatorDescriptor[] taskCommunicatorDescriptors) {
    return new ServicePluginsDescriptor(enableContainers, enableUber, taskSchedulerDescriptor,
        containerLauncherDescriptors, taskCommunicatorDescriptors);
  }

  /**
   * Create a service plugin descriptor which may have in-AM execution of tasks enabled. Container
   * execution is enabled by default
   *
   * @param enableUber whether to enable execution in the AM or not
   * @return a {@link ServicePluginsDescriptor} instance
   */
  public static ServicePluginsDescriptor create(boolean enableUber) {
    return new ServicePluginsDescriptor(true, enableUber, null, null, null);
  }


  public boolean areContainersEnabled() {
    return enableContainers;
  }

  public boolean isUberEnabled() {
    return enableUber;
  }

  public TaskSchedulerDescriptor[] getTaskSchedulerDescriptors() {
    return taskSchedulerDescriptors;
  }

  public ContainerLauncherDescriptor[] getContainerLauncherDescriptors() {
    return containerLauncherDescriptors;
  }

  public TaskCommunicatorDescriptor[] getTaskCommunicatorDescriptors() {
    return taskCommunicatorDescriptors;
  }

  @Override
  public String toString() {
    return "ServicePluginsDescriptor{" +
        "enableContainers=" + enableContainers +
        ", enableUber=" + enableUber +
        ", taskSchedulerDescriptors=" + Arrays.toString(taskSchedulerDescriptors) +
        ", containerLauncherDescriptors=" + Arrays.toString(containerLauncherDescriptors) +
        ", taskCommunicatorDescriptors=" + Arrays.toString(taskCommunicatorDescriptors) +
        '}';
  }
}