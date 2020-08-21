/*******************************************************************************
 * Copyright 2020 [BM Corporation and others.
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
package com.ibm.research.ergs.rdf4j.janusgraph;


import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.UUID;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV3d0;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper;
import org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.ibm.research.ergs.ingestion.loader.CreateGraphFoundationSchema;
import com.ibm.research.ergs.ingestion.loader.LoadRDFData;
import com.ibm.research.ergs.query.engine.SPARQLQueryProcessor;
import com.ibm.research.ergs.query.engine.SPARQLQueryProcessorJanusGraph;
import com.ibm.research.ergs.rdf4j.config.SystemConfig;

/**
 * It is used for performing operations on the JanusGraph server connection
 *
 * @author Udit Sharma
 *
 */
public class JanusGraphServerConnection implements JanusGraphConnection {
  private static final Logger logger = LoggerFactory.getLogger(JanusGraphServerConnection.class);

  private String janusGraphTableName;
  private Cluster cluster;
  private InputStream tbox;
  private Properties config;
  private LoadRDFData loadRDFData;
  private SPARQLQueryProcessor sparqlQueryProcessor;

  /**
   * It creates a new connection
   *
   * @param graphTableName Name of the table in the storage backend
   * @param tbox {@link InputStream} containing the Tbox String in owl format
   * @param config {@link InputStream} containing the configuration properties for ingestion
   */
  public JanusGraphServerConnection(String janusGraphTableName, InputStream tbox,
      InputStream configStream) {
    this.janusGraphTableName = janusGraphTableName;
    this.tbox = tbox;
    this.config = new Properties();
    try {
      if (configStream != null) {
        this.config.load(configStream);
      }
    } catch (IOException e) {
      logger.warn("Invalid ingestion configuration specified. Using default configuration");
    }
  }

  @Override
  public void initialize() {
    GryoMapper.Builder builder = GryoMapper.build().addRegistry(JanusGraphIoRegistry.getInstance());
    MessageSerializer serializer = new GryoMessageSerializerV3d0(builder);
    cluster = Cluster.build().addContactPoint(SystemConfig.getServerHostName())
        .port(SystemConfig.getServerPort()).serializer(serializer).create();
    // create template configuration if not exits
    createTemplateConfiguration();
    // create graph if not exists
    if (!isGraphExists()) {
      updateTemplateConfiguration();
      createGraph();
      createSchema();
    }
    // open graph
    openGraph();

    // create rdf loader
    this.loadRDFData = new LoadRDFData(getClient(), getTableName(), config);

    // create query processor
    this.sparqlQueryProcessor =
        new SPARQLQueryProcessorJanusGraph(getClient(), getTableName(), getTraversalName());
  }

  @Override
  public GraphTraversalSource getTraversalSource() {
    return AnonymousTraversalSource.traversal()
        .withRemote(DriverRemoteConnection.using(cluster, getTraversalName()));
  }

  @Override
  public String getTableName() {
    return janusGraphTableName;
  }

  @Override
  public boolean isInitialized() {
    return cluster != null && !cluster.isClosed();
  }

  @Override
  public void close() {
    cluster.close();
  }

  @Override
  public SPARQLQueryProcessor getSPARQLQueryProcessor() {
    return sparqlQueryProcessor;
  }

  @Override
  public LoadRDFData getRDFLoader() {
    return loadRDFData;
  }

  /**
   * Checks whether graph exists or not
   * 
   * @return
   */
  public boolean isGraphExists() {
    StringBuilder builder = new StringBuilder();
    builder.append(
        "ConfiguredGraphFactory.getGraphNames().contains(\"" + janusGraphTableName + "\")\n");
    Client client = cluster.connect(getUUID());
    boolean ret = client.submit(builder.toString()).one().getBoolean();
    client.close();
    return ret;

  }

  /**
   * creates template configuration on JanusGraph server using {@link SystemConfig} properties
   */
  private void createTemplateConfiguration() {
    StringBuilder builder = new StringBuilder();
    builder.append("if(ConfiguredGraphFactory.getTemplateConfiguration()==null){\n");
    builder.append("map = new HashMap();\n");
    builder.append("map.put(\"" + SystemConfig.STORAGE_BACKEND + "\", \""
        + SystemConfig.getStorageBackend() + "\");\n");
    builder.append("map.put(\"" + SystemConfig.STORAGE_HOSTNAME + "\", \""
        + SystemConfig.getStorageHostName() + "\");\n");
    builder.append("map.put(\"" + SystemConfig.INDEX_SEARCH_BACKEND + "\", \""
        + SystemConfig.getIndexBackend() + "\");\n");
    if (SystemConfig.getIndexBackend().equalsIgnoreCase("solr")) {
      builder.append("map.put(\"" + SystemConfig.INDEX_SEARCH_SOLR_MODE + "\", \""
          + SystemConfig.getSolrMode() + "\");\n");
      builder.append("map.put(\"" + SystemConfig.INDEX_SEARCH_SOLR_ZOOKEEPER_URL + "\", \""
          + SystemConfig.getSolrZookeeperUrl() + "\");\n");
      builder.append("map.put(\"" + SystemConfig.INDEX_SEARCH_SOLR_CONFIGSET + "\", \""
          + SystemConfig.getSolrConfigset() + "\");\n");
    } else {
      builder.append("map.put(\"" + SystemConfig.INDEX_SEARCH_DIRECTORY + "\", \""
          + SystemConfig.getIndexDirectory() + "\");\n");
    }
    builder.append("map.put(\"schema.default\", \"none\");\n");
    builder.append("map.put(\"storage.batch-loading\", true);\n");
    builder
        .append("ConfiguredGraphFactory.createTemplateConfiguration(new MapConfiguration(map));\n");
    builder.append("}\n");
    Client client = cluster.connect(getUUID());
    client.submit(builder.toString());
    client.close();
  }

  /**
   * Updates template configuration on JanusGraph server using {@link SystemConfig} properties
   */
  private void updateTemplateConfiguration() {
    StringBuilder builder = new StringBuilder();
    builder.append("map = new HashMap();\n");
    builder.append("map.put(\"" + SystemConfig.STORAGE_BACKEND + "\", \""
        + SystemConfig.getStorageBackend() + "\");\n");
    builder.append("map.put(\"" + SystemConfig.STORAGE_HOSTNAME + "\", \""
        + SystemConfig.getStorageHostName() + "\");\n");
    builder.append("map.put(\"" + SystemConfig.INDEX_SEARCH_BACKEND + "\", \""
        + SystemConfig.getIndexBackend() + "\");\n");
    if (SystemConfig.getIndexBackend().equalsIgnoreCase("solr")) {
      builder.append("map.put(\"" + SystemConfig.INDEX_SEARCH_SOLR_MODE + "\", \""
          + SystemConfig.getSolrMode() + "\");\n");
      builder.append("map.put(\"" + SystemConfig.INDEX_SEARCH_SOLR_ZOOKEEPER_URL + "\", \""
          + SystemConfig.getSolrZookeeperUrl() + "\");\n");
      builder.append("map.put(\"" + SystemConfig.INDEX_SEARCH_SOLR_CONFIGSET + "\", \""
          + SystemConfig.getSolrConfigset(janusGraphTableName) + "\");\n");
    } else {
      builder.append("map.put(\"" + SystemConfig.INDEX_SEARCH_DIRECTORY + "\", \""
          + SystemConfig.getIndexDirectory() + "\");\n");
    }
    builder.append("map.put(\"schema.default\", \"none\");\n");
    builder.append("map.put(\"storage.batch-loading\", true);\n");
    builder
        .append("ConfiguredGraphFactory.updateTemplateConfiguration(new MapConfiguration(map));\n");
    Client client = cluster.connect(getUUID());
    client.submit(builder.toString());
    client.close();
  }

  /**
   * It creates the graph on the JanusGraph server
   */
  private void createGraph() {
    StringBuilder builder = new StringBuilder();
    builder.append("ConfiguredGraphFactory.create(\"" + janusGraphTableName + "\");");
    Client client = cluster.connect(getUUID());
    client.submit(builder.toString());
    client.close();

  }

  /**
   * It opens the graph on the JanusGraph server
   */
  private void openGraph() {
    StringBuilder builder = new StringBuilder();
    builder.append("ConfiguredGraphFactory.open(\"" + janusGraphTableName + "\");");
    Client client = cluster.connect(getUUID());
    client.submit(builder.toString());
    client.close();

  }

  /**
   *
   * @return randomly generated uuid
   */
  private String getUUID() {
    UUID uuid = UUID.randomUUID();
    return uuid.toString();
  }

  /**
   *
   * @return client for the graph connection
   */
  private Client getClient() {
    return cluster.connect(getUUID());
  }

  /**
   * It creates a schema for graph. It should be invoked only after creating new graph object
   */
  private void createSchema() {
    CreateGraphFoundationSchema schemaCreator =
        new CreateGraphFoundationSchema(getClient(), janusGraphTableName, tbox, config);
    schemaCreator.createGraphFoundationSchema();
  }

  /**
   *
   * @return traversal name for the graph connection
   */
  private String getTraversalName() {
    return getTableName() + "_traversal";
  }

  @Override
  public void deleteGraph() {
    StringBuilder builder = new StringBuilder();
    builder.append("ConfiguredGraphFactory.drop(\"" + janusGraphTableName + "\");");
    Client client = cluster.connect(getUUID());
    client.submit(builder.toString());
    client.close();
  }

}
