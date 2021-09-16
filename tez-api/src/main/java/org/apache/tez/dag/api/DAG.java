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
package org.apache.tez.dag.api;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.collections4.BidiMap;
import org.apache.commons.collections4.bidimap.DualLinkedHashBidiMap;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.tez.client.CallerContext;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.security.DAGAccessControls;
import org.apache.tez.dag.api.Vertex.VertexExecutionContext;

import java.net.URI;
import java.util.*;

/**
 * Top level entity that defines the DAG (Directed Acyclic Graph) representing 
 * the data flow graph. Consists of a set of Vertices and Edges connecting the 
 * vertices. Vertices represent transformations of data and edges represent 
 * movement of data between vertices.
 */
@Public
public class DAG {
  
  final BidiMap<String, Vertex> vertices =
      new DualLinkedHashBidiMap<String, Vertex>();
  final Set<Edge> edges = Sets.newHashSet();
  final String name;
  final Collection<URI> urisForCredentials = new HashSet<URI>();
  Credentials credentials = new Credentials();
  Set<VertexGroup> vertexGroups = Sets.newHashSet();

  Set<GroupInputEdge> groupInputEdges = Sets.newHashSet();

  private DAGAccessControls dagAccessControls;
  Map<String, LocalResource> commonTaskLocalFiles = Maps.newHashMap();
  String dagInfo;
  CallerContext callerContext;
  private Map<String,String> dagConf = new HashMap<String, String>();
  private VertexExecutionContext defaultExecutionContext;

  private DAG(String name) {
    this.name = name;
  }

  /**
   * Create a DAG with the specified name.
   * @param name the name of the DAG
   * @return this {@link DAG}
   */
  public static DAG create(String name) {
    return new DAG(name);
  }

  /**
   * Set the files etc that must be provided to the tasks of this DAG
   * @param localFiles
   *          files that must be available locally for each task. These files
   *          may be regular files, archives etc. as specified by the value
   *          elements of the map.
   * @return {@link DAG}
   */
  public synchronized DAG addTaskLocalFiles(Map<String, LocalResource> localFiles) {
    Objects.requireNonNull(localFiles);
    TezCommonUtils.addAdditionalLocalResources(localFiles, commonTaskLocalFiles, "DAG " + getName());
    return this;
  }
  
  public synchronized DAG addVertex(Vertex vertex) {
    if (vertices.containsKey(vertex.getName())) {
      throw new IllegalStateException(
        "Vertex " + vertex.getName() + " already defined!");
    }
    vertices.put(vertex.getName(), vertex);
    return this;
  }

  public synchronized Vertex getVertex(String vertexName) {
    return vertices.get(vertexName);
  }
  
  /**
   * One of the methods that can be used to provide information about required
   * Credentials when running on a secure cluster. A combination of this and
   * addURIsForCredentials should be used to specify information about all
   * credentials required by a DAG. AM specific credentials are not used when
   * executing a DAG.
   * 
   * Set credentials which will be required to run this dag. This method can be
   * used if the client has already obtained some or all of the required
   * credentials.
   * 
   * @param credentials Credentials for the DAG
   * @return {@link DAG}
   */
  public synchronized DAG setCredentials(Credentials credentials) {
    this.credentials = credentials;
    return this;
  }

  /**
   * Set description info for this DAG that can be used for visualization purposes.
   * @param dagInfo JSON blob as a serialized string.
   *                Recognized keys by the UI are:
   *                    "context" - The application context in which this DAG is being used.
   *                                For example, this could be set to "Hive" or "Pig" if
   *                                this is being run as part of a Hive or Pig script.
   *                    "description" - General description on what this DAG is going to do.
   *                                In the case of Hive, this could be the SQL query text.
   * @return {@link DAG}
   */
  @Deprecated
  public synchronized DAG setDAGInfo(String dagInfo) {
    Objects.requireNonNull(dagInfo);
    this.dagInfo = dagInfo;
    return this;
  }


  /**
   * Set the Context in which Tez is being called.
   * @param callerContext Caller Context
   * @return {@link DAG}
   */
  public synchronized DAG setCallerContext(CallerContext callerContext) {
    Objects.requireNonNull(callerContext);
    this.callerContext = callerContext;
    return this;
  }

  /**
   * Create a group of vertices that share a common output. This can be used to implement 
   * unions efficiently.
   * @param name Name of the group.
   * @param members {@link Vertex} members of the group
   * @return {@link DAG}
   */
  public synchronized VertexGroup createVertexGroup(String name, Vertex... members) {
    // vertex group name should be unique.
    VertexGroup uv = new VertexGroup(name, members);
    if (!vertexGroups.add(uv)) {
      throw new IllegalStateException(
          "VertexGroup " + name + " already defined!");
    }

    return uv;
  }

  @Private
  public synchronized Credentials getCredentials() {
    return this.credentials;
  }


  /**
   * Set Access controls for the DAG. Which user/groups can view the DAG progess/history and
   * who can modify the DAG i.e. kill the DAG.
   * The owner of the Tez Session and the user submitting the DAG are super-users and have access
   * to all operations on the DAG.
   * @param accessControls Access Controls
   * @return {@link DAG}
   */
  public synchronized DAG setAccessControls(DAGAccessControls accessControls) {
    this.dagAccessControls = accessControls;
    return this;
  }

  @Private
  public synchronized DAGAccessControls getDagAccessControls() {
    return dagAccessControls;
  }

  /**
   * One of the methods that can be used to provide information about required
   * Credentials when running on a secure cluster. A combination of this and
   * setCredentials should be used to specify information about all credentials
   * required by a DAG. AM specific credentials are not used when executing a
   * DAG.
   * 
   * This method can be used to specify a list of URIs for which Credentials
   * need to be obtained so that the job can run. An incremental list of URIs
   * can be provided by making multiple calls to the method.
   * 
   * Currently, @{link credentials} can only be fetched for HDFS and other
   * {@link org.apache.hadoop.fs.FileSystem} implementations that support
   * credentials.
   * 
   * @param uris
   *          a list of {@link URI}s
   * @return {@link DAG}
   */
  public synchronized DAG addURIsForCredentials(Collection<URI> uris) {
    Objects.requireNonNull(uris, "URIs cannot be null");
    urisForCredentials.addAll(uris);
    return this;
  }

  /**
   * 
   * @return an unmodifiable list representing the URIs for which credentials
   *         are required.
   */
  @Private
  public synchronized Collection<URI> getURIsForCredentials() {
    return Collections.unmodifiableCollection(urisForCredentials);
  }
  
  @Private
  public synchronized Set<Vertex> getVertices() {
    return Collections.unmodifiableSet(this.vertices.values());
  }

  /**
   * Add an {@link Edge} connecting vertices in the DAG
   * @param edge The edge to be added
   * @return {@link DAG}
   */
  public synchronized DAG addEdge(Edge edge) {
    // Sanity checks
    if (!vertices.containsValue(edge.getInputVertex())) {
      throw new IllegalArgumentException(
        "Input vertex " + edge.getInputVertex() + " doesn't exist!");
    }
    if (!vertices.containsValue(edge.getOutputVertex())) {
      throw new IllegalArgumentException(
        "Output vertex " + edge.getOutputVertex() + " doesn't exist!");
    }
    if (edges.contains(edge)) {
      throw new IllegalArgumentException(
        "Edge " + edge + " already defined!");
    }

    // inform the vertices
    edge.getInputVertex().addOutputVertex(edge.getOutputVertex(), edge);
    edge.getOutputVertex().addInputVertex(edge.getInputVertex(), edge);

    edges.add(edge);
    return this;
  }
  
  /**
   * Add a {@link GroupInputEdge} to the DAG.
   * @param edge {@link GroupInputEdge}
   * @return {@link DAG}
   */
  public synchronized DAG addEdge(GroupInputEdge edge) {
    // Sanity checks
    if (!vertexGroups.contains(edge.getInputVertexGroup())) {
      throw new IllegalArgumentException(
        "Input vertex " + edge.getInputVertexGroup() + " doesn't exist!");
    }
    if (!vertices.containsValue(edge.getOutputVertex())) {
      throw new IllegalArgumentException(
        "Output vertex " + edge.getOutputVertex() + " doesn't exist!");
    }
    if (groupInputEdges.contains(edge)) {
      throw new IllegalArgumentException(
        "GroupInputEdge " + edge + " already defined!");
    }

    VertexGroup av = edge.getInputVertexGroup();
    av.addOutputVertex(edge.getOutputVertex(), edge);
    groupInputEdges.add(edge);
    
    // add new edge between members of VertexGroup and destVertex of the GroupInputEdge
    List<Edge> newEdges = Lists.newLinkedList();
    Vertex dstVertex = edge.getOutputVertex();
    VertexGroup uv = edge.getInputVertexGroup();
    for (Vertex member : uv.getMembers()) {
      newEdges.add(Edge.create(member, dstVertex, edge.getEdgeProperty()));
    }
    dstVertex.addGroupInput(uv.getGroupName(), uv.getGroupInfo());
    
    for (Edge e : newEdges) {
      addEdge(e);
    }
    
    return this;
  }
  
  /**
   * Get the DAG name
   * @return DAG name
   */
  public String getName() {
    return this.name;
  }

  /**
   * This is currently used to setup additional configuration parameters which will be available
   * in the DAG configuration used in the AppMaster. This API would be used for properties which
   * are used by the Tez framework while executing the DAG. As an example, the number of attempts
   * for a task.</p>
   *
   * A DAG inherits it's base properties from the ApplicationMaster within which it's running. This
   * method allows for these properties to be overridden.
   *
   * Currently, properties which are used by the task runtime, such as the task to AM
   * heartbeat interval, cannot be changed using this method. </p>
   *
   * Note: This API does not add any configuration to runtime components such as InputInitializers,
   * OutputCommitters, Inputs and Outputs.
   *
   * @param property the property name
   * @param value the value for the property
   * @return the current DAG being constructed
   */
  @InterfaceStability.Unstable
  public DAG setConf(String property, String value) {
    TezConfiguration.validateProperty(property, Scope.DAG);
    dagConf.put(property, value);
    return this;
  }

  /**
   * Set history log level for this DAG. This config overrides the default or one set at the session
   * level.
   *
   * @param historyLogLevel The ATS history log level for this DAG.
   *
   * @return this DAG
   */
  public DAG setHistoryLogLevel(HistoryLogLevel historyLogLevel) {
    return this.setConf(TezConfiguration.TEZ_HISTORY_LOGGING_LOGLEVEL, historyLogLevel.name());
  }

  /**
   * Sets the default execution context for the DAG. This can be overridden at a per Vertex level.
   * See {@link org.apache.tez.dag.api.Vertex#setExecutionContext(VertexExecutionContext)}
   *
   * @param vertexExecutionContext the default execution context for the DAG
   *
   * @return this DAG
   */
  @Public
  @InterfaceStability.Unstable
  public synchronized DAG setExecutionContext(VertexExecutionContext vertexExecutionContext) {
    this.defaultExecutionContext = vertexExecutionContext;
    return this;
  }

  @Private
  VertexExecutionContext getDefaultExecutionContext() {
    return this.defaultExecutionContext;
  }

  @Private
  @VisibleForTesting
  public Map<String,String> getDagConf() {
    return dagConf;
  }

  @Private
  public Map<String, LocalResource> getTaskLocalFiles() {
    return commonTaskLocalFiles;
  }
}
