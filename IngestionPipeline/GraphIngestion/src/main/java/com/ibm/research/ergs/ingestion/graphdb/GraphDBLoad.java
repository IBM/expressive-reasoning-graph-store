
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
package com.ibm.research.ergs.ingestion.graphdb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import org.eclipse.rdf4j.model.Literal;

/**
 * This interface contains graph related load operations.
 * 
 * @author Sumit Neelam (sumit.neelam@in.ibm.com)
 *
 */
public interface GraphDBLoad {
  /**
   * This is entry point for writing data into graph. It first creates type edges, then creates
   * vertex properties followed by edges
   * 
   * @param sourceID
   * @param typeMap
   * @param properties
   * @param edges
   * @param sameAsList
   * @param propertyKeys
   * @param conflictingLabels
   * @throws Exception
   */
  public void createCompleteNode(String sourceID, HashMap<String, ArrayList<String>> typeMap,
      HashMap<String, ArrayList<Literal>> properties, HashMap<String, ArrayList<String>> edges,
      ArrayList<String> sameAsList, HashMap<String, String> propertyKeys,
      HashMap<String, String> conflictingLabels) throws Exception;

  /**
   * Creates vertex set. This is called for degree 0 vertex from {@link TriplesBatch}
   * 
   * @param nodeIdSubList
   */
  public void createVertexSet(Set<String> nodeIdSubList);

  /**
   * This function writes schema information for new properties, edges, conflicting labels (used for
   * both property and edge), external indexing property
   * 
   * @param propertyKeys
   * @param edgeLabels
   * @param conflictingLabels
   * @param cycleNewIndexProp
   * @param cycleDeletePropertyKeys
   * @param cycleDeleteEdgeLabels
   * @param totalExtIndex
   */
  public void writeSchemaInformation(HashMap<String, String> propertyKeys,
      HashSet<String> edgeLabels, HashMap<String, String> conflictingLabels,
      HashMap<String, String> cycleNewIndexProp, HashSet<String> cycleDeletePropertyKeys,
      HashSet<String> cycleDeleteEdgeLabels, int totalExtIndex);

  /**
   * return existing property keys of graph
   * 
   * @return: property keys and their data type
   */
  public HashMap<String, String> getPropertyKeys();

  /**
   * return existing edges of graph
   * 
   * @return: set of edges
   */
  public Set<String> getEdgeLabels();

  /**
   * return existing conflicting labels of graph
   * 
   * @return: conflicting labels
   */
  public HashMap<String, String> getConflictingLabels();

  /**
   * return existing number of indexed property (of external backend) of graph
   * 
   * @return
   */
  public int getExteralIndexCount();

  /**
   * commits the data
   * 
   * @throws Exception
   */
  public void commit() throws Exception;
}
