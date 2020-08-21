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
package com.ibm.research.ergs.rdf4j.config;

import java.io.IOException;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * It is used for loading system configuration details.
 *
 * @author Udit Sharma
 *
 */
public class SystemConfig {
  private static final Logger logger = LoggerFactory.getLogger(SystemConfig.class);
  private static String storageTable;
  private static String storageBackend;
  private static String storageHostName;
  private static String workingDir;
  private static String serverHostName;
  private static int serverPort;
  private static String indexDirectory;
  private static String indexBackend;
  private static String solrMode;
  private static String solrZookeeperUrl;
  private static String solrConfigset;

  public static final String STORAGE_HBASE_TABLE = "storage.hbase.table";
  public static final String STORAGE_CQL_KEYSPACE = "storage.cql.keyspace";
  public static final String STORAGE_BACKEND = "storage.backend";
  public static final String STORAGE_HOSTNAME = "storage.hostname";
  public static final String INPUT_WORKING_DIR = "input.workingDir";
  public static final String SERVER_HOSTNAME = "server.hostname";
  public static final String SERVER_PORT = "server.port";
  public static final String INDEX_SEARCH_BACKEND = "index.search.backend";
  public static final String INDEX_SEARCH_DIRECTORY = "index.search.directory";
  public static final String INDEX_SEARCH_SOLR_MODE = "index.search.solr.mode";
  public static final String INDEX_SEARCH_SOLR_ZOOKEEPER_URL = "index.search.solr.zookeeper-url";
  public static final String INDEX_SEARCH_SOLR_CONFIGSET = "index.search.solr.configset";

  private static final String DEFAULT_STORAGE_BACKEND = "hbase";
  private static final String DEFAULT_STORAGE_HOSTNAME = "127.0.0.1";
  private static final String DEFAULT_STORAGE_TABLE = "LUBM";
  private static final String DEFAULT_INPUT_WORKING_DIR = "src/test/resources/";
  private static final String DEFAULT_SERVER_HOSTNAME = "127.0.0.1";
  private static final int DEFAULT_SERVER_PORT = 8182;
  private static final String DEFAULT_INDEX_SEARCH_BACKEND = "solr";
  private static final String DEFAULT_INDEX_SEARCH_DIRECTORY = "src/test/resources";
  private static final String DEFAULT_INDEX_SEARCH_SOLR_MODE = "cloud";
  private static final String DEFAULT_INDEX_SEARCH_SOLR_ZOOKEEPER_URL = "localhost:2181";
  private static final String DEFAULT_INDEX_SEARCH_SOLR_CONFIGSET = getSolrConfigset("default");

  static {
    Properties properties = new Properties();
    try {
      properties.load(
          Thread.currentThread().getContextClassLoader().getResourceAsStream("system.properties"));
    } catch (IOException e) {
      logger.warn("Can't find system.properties. Using defualt configuration");
    }
    storageTable =
        properties.containsKey(STORAGE_HBASE_TABLE) ? properties.getProperty(STORAGE_HBASE_TABLE)
            : DEFAULT_STORAGE_TABLE;
    storageBackend =
        properties.containsKey(STORAGE_BACKEND) ? properties.getProperty(STORAGE_BACKEND)
            : DEFAULT_STORAGE_BACKEND;
    storageHostName =
        properties.containsKey(STORAGE_HOSTNAME) ? properties.getProperty(STORAGE_HOSTNAME)
            : DEFAULT_STORAGE_HOSTNAME;
    workingDir =
        properties.containsKey(INPUT_WORKING_DIR) ? properties.getProperty(INPUT_WORKING_DIR)
            : DEFAULT_INPUT_WORKING_DIR;
    serverHostName =
        properties.containsKey(SERVER_HOSTNAME) ? properties.getProperty(SERVER_HOSTNAME)
            : DEFAULT_SERVER_HOSTNAME;
    serverPort =
        properties.containsKey(SERVER_PORT) ? Integer.parseInt(properties.getProperty(SERVER_PORT))
            : DEFAULT_SERVER_PORT;
    indexBackend =
        properties.containsKey(INDEX_SEARCH_BACKEND) ? properties.getProperty(INDEX_SEARCH_BACKEND)
            : DEFAULT_INDEX_SEARCH_BACKEND;
    indexDirectory = properties.containsKey(INDEX_SEARCH_DIRECTORY)
        ? properties.getProperty(INDEX_SEARCH_DIRECTORY)
        : DEFAULT_INDEX_SEARCH_DIRECTORY;
    solrMode = properties.containsKey(INDEX_SEARCH_SOLR_MODE)
        ? properties.getProperty(INDEX_SEARCH_SOLR_MODE)
        : DEFAULT_INDEX_SEARCH_SOLR_MODE;
    solrZookeeperUrl = properties.containsKey(INDEX_SEARCH_SOLR_ZOOKEEPER_URL)
        ? properties.getProperty(INDEX_SEARCH_SOLR_ZOOKEEPER_URL)
        : DEFAULT_INDEX_SEARCH_SOLR_ZOOKEEPER_URL;
    solrConfigset = properties.containsKey(INDEX_SEARCH_SOLR_CONFIGSET)
        ? properties.getProperty(INDEX_SEARCH_SOLR_CONFIGSET)
        : DEFAULT_INDEX_SEARCH_SOLR_CONFIGSET;

  }

  public static String getStorageBackend() {
    return storageBackend;
  }

  public static String getStorageHostName() {
    return storageHostName;
  }

  public static String getWorkingDirectory() {
    return workingDir;
  }

  public static String getWorkingDir() {
    return workingDir;
  }

  public static void setWorkingDir(String workingDir) {
    SystemConfig.workingDir = workingDir;
  }

  public static void setStorageTable(String storageHbaseTable) {
    SystemConfig.storageTable = storageHbaseTable;
  }

  public static void setStorageBackend(String storageBackend) {
    SystemConfig.storageBackend = storageBackend;
  }

  public static void setStorageHostName(String storageHostName) {
    SystemConfig.storageHostName = storageHostName;
  }

  public static String getServerHostName() {
    return serverHostName;
  }

  public static void setServerHostName(String serverHostName) {
    SystemConfig.serverHostName = serverHostName;
  }

  public static int getServerPort() {
    return serverPort;
  }

  public static void setServerPort(int serverPort) {
    SystemConfig.serverPort = serverPort;
  }

  public static String getIndexBackend() {
    return indexBackend;
  }

  public static void setIndexBackend(String indexBackend) {
    SystemConfig.indexBackend = indexBackend;
  }

  public static String getIndexDirectory() {
    return indexDirectory + "/index-data/" + storageTable;
  }

  public static void setIndexDirectory(String indexDirectory) {
    SystemConfig.indexDirectory = indexDirectory;
  }

  public static String getSolrMode() {
    return solrMode;
  }

  public static void setSolrMode(String solrMode) {
    SystemConfig.solrMode = solrMode;
  }

  public static String getSolrZookeeperUrl() {
    return solrZookeeperUrl;
  }

  public static void setSolrZookeeperUrl(String solrZookeeperUrl) {
    SystemConfig.solrZookeeperUrl = solrZookeeperUrl;
  }

  public static String getSolrConfigset() {
    return solrConfigset;
  }

  public static void setSolrConfigset(String solrConfigset) {
    SystemConfig.solrConfigset = solrConfigset;
  }

  public static String getSolrConfigset(String tableName) {
    return tableName;
  }

}
