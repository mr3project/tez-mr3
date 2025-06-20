<!--
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->
MR3
===

MR3 is a new execution engine for Hadoop and Kubernetes. Similar in spirit to
MapReduce and Tez, it is a new execution engine with simpler design, better
performance, and more features. MR3 serves as a framework for running jobs on
Hadoop and Kubernetes. MR3 also supports standalone mode which does not require
a resource manager such as Hadoop or Kubernetes.

The main application of MR3 is Hive on MR3. With MR3 as the execution engine,
the user can run Apache Hive not only on Hadoop but also directly on Kubernetes.
By exploiting standalone mode supported by MR3, the user can run Apache Hive
virtually in any type of cluster regardless of the availability of Hadoop or
Kubernetes and the version of Java installed in the system.

MR3 is implemented in Scala.

Tez for MR3
==========

Tez for MR3 is a runtime library derived from Apache Tez and significantly modified
to support MR3.

* For the runtime library compatible with Apache Hive 4.0.0 on MR3,
check out [branch `master4.0-java17`](https://github.com/mr3project/tez-mr3/tree/master4.0-java17).

For the full documentation on MR3 (including Quick Start Guide), please visit:

  https://mr3docs.datamonad.com/

* [MR3 Slack](https://join.slack.com/t/mr3-help/shared_invite/zt-1wpqztk35-AN8JRDznTkvxFIjtvhmiNg)
* [MR3 Google Group](https://groups.google.com/g/hive-mr3)

