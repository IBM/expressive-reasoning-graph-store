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
package com.ibm.research.ergs.query.utils;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Column;

/**
 * It is used for fetching type, indexing, existence information for properties in DB.
 *
 * @author Udit Sharma
 *
 */
public class PropertyMapping {
  private static String DATATYPE_PROPERTY_ID = "propertyKeys";
  private static String OBJECT_PROPERTY_ID = "edgeLabels";
  private static String CONFLICTING_PROPERTY_ID = "conflicts";
  private static String INDEX_ID = "indexProp";

  private GraphTraversalSource g;

  /**
   * Constructs {@link PropertyMapping}
   *
   * @param g traversal source
   */
  public PropertyMapping(GraphTraversalSource g) {
    this.g = g;
  }

  /**
   *
   * @param property property IRI
   * @return True if property is present in DB false otherwise
   */
  public boolean isProperty(String property) {
    return isDataTypeProperty(property) | isObjectProperty(property)
        | isConflictingProperty(property);
  }

  /**
   *
   * @param property property IRI
   * @return True if property is data type property false otherwise
   */
  public boolean isDataTypeProperty(String property) {
    try {
      return g.V().has("id", DATATYPE_PROPERTY_ID).has(property).hasNext();
    } catch (Exception e) {
      return false;
    }
  }

  /**
   *
   * @param property property IRI
   * @return True if property is object property false otherwise
   */
  public boolean isObjectProperty(String property) {
    try {
      return g.V().has("id", OBJECT_PROPERTY_ID).has(property).hasNext();
    } catch (Exception e) {
      return false;
    }
  }

  /**
   *
   * @param property property IRI
   * @return True if property is conflicting/mixed property false otherwise
   */
  public boolean isConflictingProperty(String property) {
    try {
      return g.V().has("id", CONFLICTING_PROPERTY_ID).has(property).hasNext();
    } catch (Exception e) {
      return false;
    }
  }

  /**
   *
   * @param property property IRI
   * @return True if property is indexed false otherwise
   */
  public boolean isIndexed(String property) {
    try {
      return g.V().has("id", INDEX_ID).has(property).hasNext();
    } catch (Exception e) {
      return false;
    }

  }

  /**
   *
   * @return {@link Set} of all the data type properties present in DB
   */
  public Set<String> getAllDataTypeProperties() {
    GraphTraversal<?, Collection<String>> tr =
        g.V().has("id", DATATYPE_PROPERTY_ID).valueMap().select(Column.keys);
    return new HashSet<String>(tr.next());
  }

  /**
   *
   * @return {@link Set} of all the object properties present in DB
   */
  public Set<String> getAllObjectProperties() {
    GraphTraversal<?, Collection<String>> tr =
        g.V().has("id", OBJECT_PROPERTY_ID).valueMap().select(Column.keys);
    return new HashSet<String>(tr.next());
  }

  /**
   *
   * @return {@link Set} of all the conflicting properties present in DB
   */
  public Set<String> getAllConflictingProperties() {
    GraphTraversal<?, Collection<String>> tr =
        g.V().has("id", CONFLICTING_PROPERTY_ID).valueMap().select(Column.keys);
    return new HashSet<String>(tr.next());
  }

  /**
   *
   * @return {@link String} containing TBox for given graph
   */
  public String getTbox() {
    return (String) g.V().has("id", "tbox").values("ontology").tryNext().orElse(null);
  }

}
