
/*******************************************************************************
 * Copyright 2020 IBM Corporation and others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *******************************************************************************/
package com.ibm.research.ergs.ingestion.loader;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;

/**
 * Class holds loading related setting and configuration options
 * 
 * @author Sumit Neelam (sumit.neelam@in.ibm.com)
 *
 */
public class InputParameters {
  public static String TBOXFILE;
  public static boolean BUILDSUMMARY;
  public static int COMMIT_FREQUENCY;
  public static int NUM_THREADS;

  public static String GRAPH_NAME;
  public static String EXTERNAL_INDEX_PREFIX;

  public static HashSet<String> GRAPH_INDEX;
  public static HashSet<String> EXTERNAL_INDEX_BOTH;
  public static HashSet<String> EXTERNAL_INDEX_STRING;
  public static HashSet<String> EXTERNAL_INDEX_TEXT;

  public static boolean BUILD_GRAPH_INDEX_ALL_PROPS = false;
  public static boolean BUILD_EXTERNAL_INDEX_ALL_PROPS = false;
  public static String ALL_EXTERNAL_INDEXTYPE;

  public static boolean BUILD_ALLPROP_INDEX = false;

  public static boolean ENABLE_FORWARD_CHAINING = false;

  public static boolean ADV_LOADING = true;

  public static boolean IS_FULL_ABOX = true;

  public static void setParemeters(Properties properties, String graphName) {
    TBOXFILE = properties.getProperty("input.ontology");
    String buildSummary = properties.getProperty("input.buildsummary", "false");
    String commitFrequency = properties.getProperty("input.commitfrequency", "10000");
    String numThreads = properties.getProperty("input.numthreads", "1");
    String graphIndex = properties.getProperty("input.indexprop");
    String textExternalIndex = properties.getProperty("input.textindexprop");
    String stringExternalIndex = properties.getProperty("input.stringindexprop");
    String textStringExternalIndex = properties.getProperty("input.textstringindexprop");
    String buildAllPropIndex = properties.getProperty("input.buildallpropindex", "false");
    String enableForwardChaining = properties.getProperty("input.enableforwardchaining", "false");

    GRAPH_INDEX = new HashSet<String>();
    EXTERNAL_INDEX_BOTH = new HashSet<String>();
    EXTERNAL_INDEX_STRING = new HashSet<String>();
    EXTERNAL_INDEX_TEXT = new HashSet<String>();

    BUILDSUMMARY = false;

    GRAPH_NAME = graphName;

    if (buildSummary != null) {
      if (buildSummary.equalsIgnoreCase("true")) {
        BUILDSUMMARY = true;
      }
    }
    if (graphIndex != null) {
      if (graphIndex.equalsIgnoreCase("all")) {
        BUILD_GRAPH_INDEX_ALL_PROPS = true;
      } else {
        GRAPH_INDEX.addAll(Arrays.asList(graphIndex.split(",")));
      }
    }
    if (textExternalIndex != null) {
      if (textExternalIndex.equalsIgnoreCase("all")) {
        BUILD_EXTERNAL_INDEX_ALL_PROPS = true;
        ALL_EXTERNAL_INDEXTYPE = "text";
      } else {
        EXTERNAL_INDEX_TEXT.addAll(Arrays.asList(textExternalIndex.split(",")));
      }
    }
    if (stringExternalIndex != null) {
      if (stringExternalIndex.equalsIgnoreCase("all")) {
        BUILD_EXTERNAL_INDEX_ALL_PROPS = true;
        ALL_EXTERNAL_INDEXTYPE = "string";
      } else {
        EXTERNAL_INDEX_STRING.addAll(Arrays.asList(stringExternalIndex.split(",")));
      }
    }
    if (textStringExternalIndex != null) {
      if (textStringExternalIndex.equalsIgnoreCase("all")) {
        BUILD_EXTERNAL_INDEX_ALL_PROPS = true;
        ALL_EXTERNAL_INDEXTYPE = "textstring";
      } else {
        EXTERNAL_INDEX_BOTH.addAll(Arrays.asList(textStringExternalIndex.split(",")));
      }
    }
    if (buildAllPropIndex != null) {
      if (buildAllPropIndex.equalsIgnoreCase("true")) {
        BUILD_ALLPROP_INDEX = true;
      }
    }
    if (enableForwardChaining != null) {
      if (enableForwardChaining.equalsIgnoreCase("true")) {
        ENABLE_FORWARD_CHAINING = true;
      }
    }
    COMMIT_FREQUENCY = Integer.parseInt(commitFrequency);
    NUM_THREADS = Integer.parseInt(numThreads);
    EXTERNAL_INDEX_PREFIX = graphName + "Index";
  }
}
