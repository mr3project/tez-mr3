/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.tez.common;

import java.io.File;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringInterner;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezConstants;

@Private
public class TezYARNUtils {
  private static Logger LOG = LoggerFactory.getLogger(TezYARNUtils.class);

  public static final String ENV_NAME_REGEX = "[A-Za-z_][A-Za-z0-9_]*";

  private static final Pattern VAR_SUBBER =
    Pattern.compile(Shell.getEnvironmentVariableRegex());
  private static final Pattern VARVAL_SPLITTER = Pattern.compile(
    "(?<=^|,)"                            // preceded by ',' or line begin
      + '(' + ENV_NAME_REGEX + ')'      // var group
      + '='
      + "([^,]*)"                             // val group
  );

  public static void appendToEnvFromInputString(Map<String, String> env,
      String envString, String classPathSeparator) {
    if (envString != null && envString.length() > 0) {
      Matcher varValMatcher = VARVAL_SPLITTER.matcher(envString);
      while (varValMatcher.find()) {
        String envVar = varValMatcher.group(1);
        Matcher m = VAR_SUBBER.matcher(varValMatcher.group(2));
        StringBuffer sb = new StringBuffer();
        while (m.find()) {
          String var = m.group(1);
          // replace $env with the child's env constructed by tt's
          String replace = env.get(var);
          // if this key is not configured by the tt for the child .. get it
          // from the tt's env
          if (replace == null)
            replace = System.getenv(var);
          // If the env key is not present leave it as it is and assume it will
          // be set by YARN ContainerLauncher. For eg: $HADOOP_COMMON_HOME
          if (replace != null)
            m.appendReplacement(sb, Matcher.quoteReplacement(replace));
        }
        m.appendTail(sb);
        addToEnvironment(env, envVar, sb.toString(), classPathSeparator);
      }
    }
  }
  
  public static void addToEnvironment(
      Map<String, String> environment,
      String variable, String value, String classPathSeparator) {
    String val = environment.get(variable);
    if (val == null) {
      val = value;
    } else {
      val = val + classPathSeparator + value;
    }
    environment.put(StringInterner.weakIntern(variable), 
        StringInterner.weakIntern(val));
  }

}
