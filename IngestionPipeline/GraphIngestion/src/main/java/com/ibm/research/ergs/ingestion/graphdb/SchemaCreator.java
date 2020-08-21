
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

/**
 * It holds scheme creation functions.
 * 
 * @author Sumit Neelam (sumit.neelam@in.ibm.com)
 *
 */
public interface SchemaCreator {
  /**
   * It creates JanusGraph property.
   * 
   * @param propertyName: property name
   * @param propertyType: property datatype
   * @param multivalued: is multivalued
   * @param isSet: multivalued property should contains unique values.
   */
  public void createProperty(String propertyName, String propertyType, boolean multivalued,
      boolean isSet);

  /**
   * It creates JanusGraph indexed property.
   * 
   * @param propertyName: property name
   * @param propertyType: property datatype
   * @param isUnique
   * @param multivalued: is multivalued
   * @param isSet: multivalued property should contains unique values.
   */
  public void createIndexProperty(String propertyName, String propertyType, boolean isUnique,
      boolean multivalued, boolean isSet);

  /**
   * It creates JanusGraph external indexed property.
   * 
   * @param propertyName: property name
   * @param indexName: name of index in indexing backend
   * @param propertyType: property datatype
   * @param multivalued: is multivalued
   * @param isSet: multivalued property should contains unique values.
   * @param indexType: type values {"text", "string", "textstring"
   */
  public void createExternalIndexProperty(String propertyName, String indexName,
      String propertyType, boolean multivalued, boolean isSet, String indexType);

  /**
   * It creates JanusGraph indexed edge property.
   * 
   * @param propertyName: property name
   * @param propertyType: property datatype
   * @param isUnique
   * @param multivalued: is multivalued
   * @param isSet: multivalued property should contains unique values.
   */
  public void createEdgeIndexProperty(String propertyName, String propertyType, boolean isUnique);

  /**
   * It creates JanusGraph edge.
   * 
   * @param edgeLabel
   */
  public void createEdgeLabel(String edgeLabel);

  /**
   * It creates JanusGraph simple edge, i.e., avoids parallel edge for edgeLabel.
   * 
   * @param edgeLabel
   */
  public void createSimpleEdgeLabel(String edgeLabel);

  /**
   * commits graph creation operations
   */
  public void commit();
}
