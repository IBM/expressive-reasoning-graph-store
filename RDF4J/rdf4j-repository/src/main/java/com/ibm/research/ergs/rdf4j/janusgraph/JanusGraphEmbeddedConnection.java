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
package com.ibm.research.ergs.rdf4j.janusgraph;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphFactory.Builder;
import org.janusgraph.diskstorage.BackendException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.ibm.research.ergs.ingestion.graphdb.JanusGraphSchemaConstant;
import com.ibm.research.ergs.ingestion.graphdb.SchemaCreatorDirectMode;
import com.ibm.research.ergs.ingestion.loader.CreateGraphFoundationSchema;
import com.ibm.research.ergs.ingestion.loader.LoadRDFData;
import com.ibm.research.ergs.query.engine.SPARQLQueryProcessor;
import com.ibm.research.ergs.query.engine.SPARQLQueryProcessorJanusGraph;
import com.ibm.research.ergs.rdf4j.config.SystemConfig;

/**
 * It is used for performing operations on the JanusGraph local connection
 *
 * @author Udit Sharma
 *
 */
public class JanusGraphEmbeddedConnection implements JanusGraphConnection {
  private static final Logger logger = LoggerFactory.getLogger(JanusGraphEmbeddedConnection.class);

  private JanusGraph graph;
  private String graphTableName;
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
  public JanusGraphEmbeddedConnection(String graphTableName, InputStream tbox,
      InputStream configStream) {
    this.graphTableName = graphTableName;
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
    graph = getConfiguration().open();
    if (!isGraphExists()) {
      createSchema();
    }
    sparqlQueryProcessor = new SPARQLQueryProcessorJanusGraph(graph, graphTableName);
    loadRDFData = new LoadRDFData(graph, config);
  }

  @Override
  public GraphTraversalSource getTraversalSource() {
    return graph.traversal();
  }

  @Override
  public String getTableName() {
    return graphTableName;
  }

  @Override
  public boolean isInitialized() {
    return graph != null && graph.isOpen();
  }

  @Override
  public void close() {
    // graph.close();
  }

  /**
   * It generates graph builder by fetching different {@link SystemConfig} properties
   * 
   * @return janusgraph builder
   */
  private Builder getConfiguration() {
    Builder builder = JanusGraphFactory.build()
        .set(SystemConfig.STORAGE_BACKEND, SystemConfig.getStorageBackend())
        .set(SystemConfig.STORAGE_HOSTNAME, SystemConfig.getStorageHostName())
        .set(SystemConfig.INDEX_SEARCH_BACKEND, SystemConfig.getIndexBackend());

    if (SystemConfig.getStorageBackend().equalsIgnoreCase("hbase")) {
      builder.set(SystemConfig.STORAGE_HBASE_TABLE, graphTableName);
    } else {
      builder.set(SystemConfig.STORAGE_CQL_KEYSPACE, graphTableName);
    }

    if (SystemConfig.getIndexBackend().equalsIgnoreCase("solr")) {
      builder.set(SystemConfig.INDEX_SEARCH_SOLR_MODE, SystemConfig.getSolrMode());
      builder.set(SystemConfig.INDEX_SEARCH_SOLR_ZOOKEEPER_URL, SystemConfig.getSolrZookeeperUrl());
      builder.set(SystemConfig.INDEX_SEARCH_SOLR_CONFIGSET, SystemConfig.getSolrConfigset());
    } else {
      builder.set(SystemConfig.INDEX_SEARCH_DIRECTORY, SystemConfig.getIndexDirectory());
    }
    builder.set("schema.default", "none");
    builder.set("storage.batch-loading", true);
    return builder;

  }

  /**
   * It creates a schema for graph. It should be invoked only after creating new graph object
   */
  private void createSchema() {
    CreateGraphFoundationSchema schemaCreator =
        new CreateGraphFoundationSchema(graph, tbox, config);
    schemaCreator.createGraphFoundationSchema();
  }

  /**
   * It checks whether the graph already exist in the database
   * 
   * @return
   */
  private boolean isGraphExists() {
    SchemaCreatorDirectMode schemaCreator = new SchemaCreatorDirectMode(graph.openManagement());
    return schemaCreator.isPropertyExists(JanusGraphSchemaConstant.ID_PROPERTY);
  }

  @Override
  public SPARQLQueryProcessor getSPARQLQueryProcessor() {
    return sparqlQueryProcessor;
  }

  @Override
  public LoadRDFData getRDFLoader() {
    return loadRDFData;
  }

  @Override
  public void deleteGraph() {
    try {
      JanusGraphFactory.drop(graph);
    } catch (BackendException e) {
      e.printStackTrace();
    }

  }

}
