/*******************************************************************************
 * Copyright 2020 IBM Corporation and others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package com.ibm.research.ergs.rdf4j.repository;

import java.io.File;
import java.io.InputStream;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import com.ibm.research.ergs.rdf4j.config.SystemConfig;
import com.ibm.research.ergs.rdf4j.janusgraph.JanusGraphConnection;
import com.ibm.research.ergs.rdf4j.janusgraph.JanusGraphEmbeddedConnection;
import com.ibm.research.ergs.rdf4j.janusgraph.JanusGraphServerConnection;

/**
 * It is used for creating ERGS {@link Repository}
 *
 * @author Udit Sharma
 *
 */
public class ExpressiveReasoningGraphStoreRepository implements Repository {
  private JanusGraphConnection janusGraphConnection;
  boolean initialized = false;

  /**
   * constructor for creating ERGS {@link Repository}
   *
   * @param janusGraphTableName name of table/graph
   * @param tbox {@link InputStream} containing TBpx string in OWL format. It is used for forward
   *        chaining/query expansion. If TBpx is null no inferencing is performed.
   * @param config {@link InputStream} containing properties file with different configuration
   *        parameters. It is used during ingestion. If it is null default options are used.
   * @param serverMode flag used for Server/Local mode. If serverMode is True JanusGraph is used in
   *        server mode else it is used in local mode
   */
  public ExpressiveReasoningGraphStoreRepository(String janusGraphTableName, InputStream tbox, InputStream config,
      boolean serverMode) {
    SystemConfig.setStorageTable(janusGraphTableName);
    janusGraphConnection =
        serverMode ? new JanusGraphServerConnection(janusGraphTableName, tbox, config)
            : new JanusGraphEmbeddedConnection(janusGraphTableName, tbox, config);
  }

  /**
   * constructor for creating ERGS {@link Repository} without inferencing support and default
   * configuration properties
   *
   * @param janusGraphTableName name of table/graph
   * @param serverMode flag used for Server/Local mode. If serverMode is True JanusGraph is used in
   *        server mode else it is used in local mode
   */
  public ExpressiveReasoningGraphStoreRepository(String janusGraphTableName, boolean serverMode) {
    this(janusGraphTableName, null, null, serverMode);
  }

  /**
   * constructor for creating ERGS {@link Repository} without inferencing support and default
   * configuration properties using JanusGraph server.
   *
   * @param janusGraphTableName
   */
  public ExpressiveReasoningGraphStoreRepository(String janusGraphTableName) {
    this(janusGraphTableName, true);
  }

  @Override
  public RepositoryConnection getConnection() throws RepositoryException {
    if (!janusGraphConnection.isInitialized()) {
      janusGraphConnection.initialize();
    }
    ExpressiveReasoningGraphStoreRepositoryConnection janusGraphRepositoryConnection =
        new ExpressiveReasoningGraphStoreRepositoryConnection(janusGraphConnection, this);
    return janusGraphRepositoryConnection;
  }

  @Override
  public File getDataDir() {
    throw new RepositoryException("JanusGraph does not have a data directory");
  }

  @Override
  public ValueFactory getValueFactory() {
    return SimpleValueFactory.getInstance();
  }

  @Override
  public void init() {
    janusGraphConnection.initialize();
  }

  @Override
  public void initialize() throws RepositoryException {
    janusGraphConnection.initialize();
  }

  @Override
  public boolean isInitialized() {
    return janusGraphConnection.isInitialized();
  }

  @Override
  public boolean isWritable() throws RepositoryException {
    return false;
  }

  @Override
  public void setDataDir(File dataDir) {
    // throw new RepositoryException("JanusGraph does not have a data
    // directory");
  }

  @Override
  public void shutDown() throws RepositoryException {
    janusGraphConnection.close();
  }
}
