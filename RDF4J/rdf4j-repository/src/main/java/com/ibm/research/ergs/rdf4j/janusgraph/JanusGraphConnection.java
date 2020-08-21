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
package com.ibm.research.ergs.rdf4j.janusgraph;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import com.ibm.research.ergs.ingestion.loader.LoadRDFData;
import com.ibm.research.ergs.query.engine.SPARQLQueryProcessor;

/**
 * It is used for performing operations on the graph connection
 *
 * @author Udit Sharma
 *
 */
public interface JanusGraphConnection {

	/**
	 * It initializes the connection
	 */
	public void initialize();

	/**
	 * It return traversal object for the graph connection
	 *
	 * @return graph traversal source
	 */
	public GraphTraversalSource getTraversalSource();

	/**
	 * It returns the graph/table name used by the graph connection
	 *
	 * @return graph name
	 */
	public String getTableName();

	/**
	 * It checks whether the connection is initialized or not
	 *
	 * @return flag containing initialization status
	 */
	public boolean isInitialized();

	/**
	 * It closes the connection
	 */
	public void close();

	/**
	 * It deletes the graph/table associated with graph connection
	 */
	public void deleteGraph();

	/**
	 * It returns query processor for executing SPARQL queries
	 * 
	 * @return SPARQLQueryProcessor object
	 */
	public SPARQLQueryProcessor getSPARQLQueryProcessor();

	/**
	 * It returns RDFF loader for loading RDF data
	 * 
	 * @return LoadRDFData object
	 */
	public LoadRDFData getRDFLoader();

}
