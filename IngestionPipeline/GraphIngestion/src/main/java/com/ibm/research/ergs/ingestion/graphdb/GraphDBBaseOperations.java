
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

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.eclipse.rdf4j.model.Literal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.ibm.research.ergs.ingestion.loader.ConstantParameters;

/**
 * This class is base for different loading classes. It contains graph base operationss.
 * 
 * @author Sumit Neelam (sumit.neelam@in.ibm.com)
 *
 */
public class GraphDBBaseOperations {

  private static final Logger logger = LoggerFactory.getLogger(GraphDBBaseOperations.class);

  public GraphTraversalSource g;
  public ConnectionMode connectionMode;
  private HashMap<String, Vertex> typeNodes;
  private HashMap<String, Long> edgeInstanceCount;
  private HashMap<String, Long> edgeDomainCount;
  private HashMap<String, Long> edgeRangeCount;

  /**
   * Consstructs {@link GraphDBBaseOperations}
   * 
   * @param g
   * @param connectionMode
   */
  public GraphDBBaseOperations(GraphTraversalSource g, ConnectionMode connectionMode) {
    this.g = g;
    this.connectionMode = connectionMode;

    typeNodes = new HashMap<>();
    edgeInstanceCount = new HashMap<>();
    edgeDomainCount = new HashMap<>();
    edgeRangeCount = new HashMap<>();
  }

  /**
   * adds new vertex with label
   * 
   * @param label: vertex label
   * @return: created vertex
   */
  Vertex addVertex(String label) {
    Vertex vertex;
    if (label != null) {
      vertex = g.addV(label).next();
    } else {
      vertex = g.addV().next();
    }
    return (vertex);
  }

  /**
   * creates edge from source to target with label as edgeLabel.
   * 
   * @param source
   * @param target
   * @param edgeLabel
   * @return: edge
   */
  public Edge addEdge(Vertex source, Vertex target, String edgeLabel) {
    Edge edge = g.V(source).as("s").V(target).addE(edgeLabel).from("s").next();
    return (edge);
  }

  /**
   * creates edge from source to target (using their ids) with label as edgeLabel.
   * 
   * @param sourceID
   * @param targetID
   * @param edgeLabel
   * @return: edge
   */
  public Edge addEdge(String sourceID, String targetID, String edgeLabel) {
    Edge edge = g.V().has(JanusGraphSchemaConstant.ID_PROPERTY, sourceID).as("s").V()
        .has(JanusGraphSchemaConstant.ID_PROPERTY, targetID).addE(edgeLabel).from("s").next();
    return (edge);
  }

  /**
   * adds <propertyKey, propertyValue> to vertex.
   * 
   * @param vertex
   * @param propertyKey
   * @param propertyValue
   */
  public void addProperty(Vertex vertex, String propertyKey, Object propertyValue) {
    g.V(vertex).property(propertyKey, propertyValue).next();
  }

  /**
   * adds <propertyKey, propertyValue> to vertex (using its id).
   * 
   * @param vertexId
   * @param propertyKey
   * @param propertyValue
   */
  public void addProperty(String vertexId, String propertyKey, Object propertyValue) {
    g.V().has(JanusGraphSchemaConstant.ID_PROPERTY, vertexId).property(propertyKey, propertyValue)
        .next();
  }

  /**
   * This function returns vertex with propertyKey = propertyVal. It first checks for existing
   * vertex, if vertex does not exist then new vertex is created with given label.
   * 
   * @param propertyKey
   * @param propertyValue
   * @param label
   * @return
   */
  public Vertex getVertex(String propertyKey, Object propertyValue, String label) {
    Vertex vertex;
    Optional<Vertex> optionalVertex = g.V().has(propertyKey, propertyValue).tryNext();
    if (optionalVertex.isPresent()) {
      vertex = optionalVertex.get();
    } else {
      if (label != null) {
        vertex = g.addV(label).property(JanusGraphSchemaConstant.ID_PROPERTY, propertyValue)
            .property(JanusGraphSchemaConstant.ORG_ID_PROPERTY, propertyValue).next();
      } else {
        vertex = g.addV().property(JanusGraphSchemaConstant.ID_PROPERTY, propertyValue)
            .property(JanusGraphSchemaConstant.ORG_ID_PROPERTY, propertyValue).next();
      }
    }
    return (vertex);
  }

  /**
   * this is wrapper to above method without label of vertex.
   * 
   * @param propertyKey
   * @param propertyValue
   * @return
   */
  public Vertex getVertex(String propertyKey, Object propertyValue) {
    return (getVertex(propertyKey, propertyValue, null));
  }

  /**
   * This function returns type node. It maintains map of type cached vertices. It first checks type
   * vertex presence in map otherwise get from database.
   * 
   * @param type
   * @return
   */
  public Vertex getTypeNode(String type) {
    if (typeNodes.containsKey(type)) {
      return (typeNodes.get(type));
    } else {

      /*
       * using id as primary key for type vertex.
       */
      Vertex typeNode = getVertex(JanusGraphSchemaConstant.ORG_ID_PROPERTY, type);
      typeNodes.put(type, typeNode);
      return (typeNode);
    }
  }

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
      HashSet<String> cycleDeleteEdgeLabels, int totalExtIndex) {
    Vertex edgeNode = getVertex(JanusGraphSchemaConstant.ID_PROPERTY, "edgeLabels");
    Vertex propertyNode = getVertex(JanusGraphSchemaConstant.ID_PROPERTY, "propertyKeys");
    Vertex conflictNode = getVertex(JanusGraphSchemaConstant.ID_PROPERTY, "conflicts");
    Vertex indexNode = getVertex(JanusGraphSchemaConstant.ID_PROPERTY, "indexProp");

    Set<String> tempDeletePropertyKey = new HashSet<String>(cycleDeletePropertyKeys);
    for (String deleteProperty : tempDeletePropertyKey) {
      if (propertyKeys.containsKey(deleteProperty)) {
        cycleDeletePropertyKeys.remove(deleteProperty);
        propertyKeys.remove(deleteProperty);
      }
    }
    Set<String> tempDeleteEdgeLabel = new HashSet<String>(cycleDeleteEdgeLabels);
    for (String deleteEdgeLabel : tempDeleteEdgeLabel) {
      if (edgeLabels.contains(deleteEdgeLabel)) {
        cycleDeleteEdgeLabels.remove(deleteEdgeLabel);
        edgeLabels.remove(deleteEdgeLabel);
      }
    }
    for (String edgeLabel : edgeLabels) {
      addProperty(edgeNode, edgeLabel, "MULTI");
      // create edge prop vertex
      getVertex(JanusGraphSchemaConstant.ID_PROPERTY, edgeLabel);
    }
    for (String propertyKey : propertyKeys.keySet()) {
      addProperty(propertyNode, propertyKey, propertyKeys.get(propertyKey));
      // create prop key vertex.
      getVertex(JanusGraphSchemaConstant.ID_PROPERTY, propertyKey);
    }
    for (String elementKey : conflictingLabels.keySet()) {
      addProperty(conflictNode, elementKey, conflictingLabels.get(elementKey));
    }
    for (String propertyKey : cycleDeletePropertyKeys) {
      g.V(propertyNode).properties(propertyKey).drop();
    }
    for (String edgeLable : cycleDeleteEdgeLabels) {
      g.V(edgeNode).properties(edgeLable).drop();
    }
    for (String propertyKey : cycleNewIndexProp.keySet()) {
      addProperty(indexNode, propertyKey, cycleNewIndexProp.get(propertyKey));
    }
    addProperty(indexNode, "totalExtIndex", totalExtIndex);
  }

  /**
   * return existing property keys of graph
   * 
   * @return: property keys and their data type
   */
  public HashMap<String, String> getPropertyKeys() {
    HashMap<String, String> propertyKeys = new HashMap<String, String>();
    GraphTraversal<Vertex, Map<Object, Object>> t =
        g.V().has(JanusGraphSchemaConstant.ID_PROPERTY, "propertyKeys").valueMap();
    if (t.hasNext()) {
      Map<Object, Object> keyVal = t.next();
      for (Object key : keyVal.keySet()) {
        if (!(key.equals(JanusGraphSchemaConstant.ID_PROPERTY)
            || key.equals(JanusGraphSchemaConstant.ORG_ID_PROPERTY))) {
          Object val = keyVal.get(key);
          if (val instanceof ArrayList) {
            ArrayList<String> list = (ArrayList<String>) val;
            propertyKeys.put((String) key, list.get(0));
          } else {
            propertyKeys.put((String) key, (String) val);
          }
        }
      }
    }
    return (propertyKeys);
  }

  /**
   * return existing edges of graph
   * 
   * @return: set of edges
   */
  public Set<String> getEdgeLabels() {
    Set<String> keys = new HashSet<String>();
    GraphTraversal<Vertex, String> t =
        g.V().has(JanusGraphSchemaConstant.ID_PROPERTY, "edgeLabels").properties().key();
    while (t.hasNext()) {
      keys.add(t.next());
    }
    keys.remove(JanusGraphSchemaConstant.ID_PROPERTY);
    keys.remove(JanusGraphSchemaConstant.ORG_ID_PROPERTY);
    return (keys);
  }

  /**
   * return existing conflicting labels of graph
   * 
   * @return: conflicting labels
   */
  public HashMap<String, String> getConflictingLabels() {
    HashMap<String, String> conflictingLabels = new HashMap<String, String>();
    GraphTraversal<Vertex, Map<Object, Object>> t =
        g.V().has(JanusGraphSchemaConstant.ID_PROPERTY, "conflicts").valueMap();
    if (t.hasNext()) {
      Map<Object, Object> keyVal = t.next();
      for (Object key : keyVal.keySet()) {
        if (!(key.equals(JanusGraphSchemaConstant.ID_PROPERTY)
            || key.equals(JanusGraphSchemaConstant.ORG_ID_PROPERTY))) {
          Object val = keyVal.get(key);
          if (val instanceof ArrayList) {
            ArrayList<String> list = (ArrayList<String>) val;
            conflictingLabels.put((String) key, list.get(0));
          } else {
            conflictingLabels.put((String) key, (String) val);
          }
        }
      }
    }
    return (conflictingLabels);
  }

  /**
   * return existing number of indexed property (of external backend) of graph
   * 
   * @return
   */
  public int getExteralIndexCount() {
    GraphTraversal<Vertex, Object> t =
        g.V().has(JanusGraphSchemaConstant.ID_PROPERTY, "indexProp").values("totalExtIndex");
    if (t.hasNext()) {
      return ((int) t.next());
    } else {
      return (0);
    }
  }

  /**
   * Creates vertex set. This is called for degree 0 vertex from {@link TriplesBatch}
   * 
   * @param nodeIdSubList
   */
  public void createVertexSet(Set<String> nodeIdSubList) {
    for (String nodeId : nodeIdSubList) {
      getVertex(JanusGraphSchemaConstant.ID_PROPERTY, nodeId);
    }
  }

  /**
   * This function adds properties to vertex.
   * 
   * @param vertex: vertex
   * @param properties: Map of properties
   * @param propertyKeys: property key to datatype map
   * @param conflictingLabels: conflicting labels to datatype map
   */
  public void addVertexProperties(Vertex vertex, HashMap<String, ArrayList<Literal>> properties,
      HashMap<String, String> propertyKeys, HashMap<String, String> conflictingLabels) {
    for (String propertyKey : properties.keySet()) {
      ArrayList<Literal> propLiteralList = properties.get(propertyKey);

      String datatype = propertyKeys.get(propertyKey);
      if (datatype == null) {
        datatype = conflictingLabels.get(propertyKey);
        if (datatype == null) {
          logger.error("ERROR: Internal Error E2");
          logger.error("Skipping property " + propertyKey + " for Vertex " + vertex);
          continue;
        }
      }

      String graphPropKey = propertyKey + "_prop";
      addProperty(vertex, "label_prop", graphPropKey);

      for (Literal propLiteral : propLiteralList) {
        String propVal = propLiteral.getLabel();

        if (datatype.equalsIgnoreCase("string")) {
          addProperty(vertex, graphPropKey, propVal);
        } else if (datatype.equalsIgnoreCase("integer")
            || datatype.equalsIgnoreCase("negativeInteger")
            || datatype.equalsIgnoreCase("nonNegativeInteger")
            || datatype.equalsIgnoreCase("nonPositiveInteger")
            || datatype.equalsIgnoreCase("positiveInteger")) {
          addProperty(vertex, graphPropKey, Integer.parseInt(propVal));
        } else if (datatype.equalsIgnoreCase("decimal") || datatype.equalsIgnoreCase("float")) {
          addProperty(vertex, graphPropKey, Float.parseFloat(propVal));
        } else if (datatype.equalsIgnoreCase("double")) {
          addProperty(vertex, graphPropKey, Double.parseDouble(propVal));
        } else if (datatype.equalsIgnoreCase("boolean")) {
          addProperty(vertex, graphPropKey, Boolean.parseBoolean(propVal));
        } else if (datatype.equalsIgnoreCase("date")) {
          Date date = getDate_yyyyMMdd(propVal);
          if (date != null) {
            addProperty(vertex, graphPropKey, date);
          }
        } else if (datatype.equalsIgnoreCase("gYear")) {
          Date date = getDate_yyyy(propVal);
          if (date != null) {
            addProperty(vertex, graphPropKey, date);
          }
        } else if (datatype.equalsIgnoreCase("gYearMonth")) {
          Date date = getDate_yyyyMM(propVal);
          if (date != null) {
            addProperty(vertex, graphPropKey, date);
          }
        } else {
          addProperty(vertex, graphPropKey, propVal);
        }

        // Storing language and datatype as meta property.
        Optional<String> language = propLiteral.getLanguage();
        if (language.isPresent()) {
          g.V(vertex).properties(graphPropKey).property("language", language.get()).next();
        }
        g.V(vertex).properties(graphPropKey)
            .property("datatype", propLiteral.getDatatype().toString()).next();
      }
    }
  }

  /**
   * converts string(yyyyMMdd) to date
   * 
   * @param dateString
   * @return
   */
  public Date getDate_yyyyMMdd(String dateString) {
    try {
      Date date = new SimpleDateFormat("yyyy-MM-dd").parse(dateString);
      return (date);
    } catch (Exception e) {
      logger.error("ERROR: Exception in parsing date (yyyy-MM-dd): " + dateString, e);
      return (null);
    }
  }

  /**
   * converts string(yyyy) to date
   * 
   * @param dateString
   * @return
   */
  public Date getDate_yyyy(String dateString) {
    try {
      Date date = new SimpleDateFormat("yyyy").parse(dateString);
      return (date);
    } catch (Exception e) {
      logger.error("ERROR: Exception in parsing date (yyyy): " + dateString, e);
      return (null);
    }
  }

  /**
   * converts string(yyyyMM) to date
   * 
   * @param dateString
   * @return
   */
  public Date getDate_yyyyMM(String dateString) {
    try {
      Date date = new SimpleDateFormat("yyyy-MM").parse(dateString);
      return (date);
    } catch (Exception e) {
      logger.error("ERROR: Exception in parsing date (yyyy-MM): " + dateString, e);
      return (null);
    }
  }

  /**
   * This function writes edge statistics (instance count, domain count, range count)
   * 
   * @param edgeLabel
   * @param statInstanceCount
   * @param statDomainCount
   * @param statRangeCount
   */
  public void writeEdgeStatistics(String edgeLabel, long statInstanceCount, long statDomainCount,
      long statRangeCount) {
    if (ConstantParameters.COLLECT_STATISTICS) {

      if (edgeInstanceCount.containsKey(edgeLabel)) {
        long oldVal = edgeInstanceCount.get(edgeLabel);
        edgeInstanceCount.put(edgeLabel, (oldVal + statInstanceCount));
      } else {
        edgeInstanceCount.put(edgeLabel, statInstanceCount);
      }
      if (edgeDomainCount.containsKey(edgeLabel)) {
        long oldVal = edgeDomainCount.get(edgeLabel);
        edgeDomainCount.put(edgeLabel, (oldVal + statDomainCount));
      } else {
        edgeDomainCount.put(edgeLabel, statDomainCount);
      }
      if (edgeRangeCount.containsKey(edgeLabel)) {
        long oldVal = edgeRangeCount.get(edgeLabel);
        edgeRangeCount.put(edgeLabel, (oldVal + statRangeCount));
      } else {
        edgeRangeCount.put(edgeLabel, statRangeCount);
      }
    }
  }

  /**
   * This function returns propertyKey value of vertex
   * 
   * @param vertex
   * @param propertyKey
   * @return: propertyValue
   */
  private Object getPropertyValue(Vertex vertex, String propertyKey) {
    GraphTraversal<Vertex, Object> t = g.V(vertex).values(propertyKey);
    if (t.hasNext()) {
      Object value = t.next();
      return (value);
    }
    return (null);
  }

  /**
   * writes property key statistics and its unique values.
   * 
   * @param graphPropertyKey
   * @param statInstanceCount
   * @param propertyValues
   */
  public void writePropertyStatistics(String graphPropertyKey, long statInstanceCount,
      Set<String> propertyValues) {
    if (ConstantParameters.COLLECT_STATISTICS) {
      Vertex elementVertex = getVertex(JanusGraphSchemaConstant.ID_PROPERTY, graphPropertyKey);
      if (statInstanceCount > 0) {
        long oldVal;
        Iterator<Long> itr =
            elementVertex.values(JanusGraphSchemaConstant.STATNODE_INSTANCE_COUNT_KEY);
        if (itr.hasNext()) {
          oldVal = itr.next();
        } else {
          oldVal = 0;
        }
        elementVertex.property(JanusGraphSchemaConstant.STATNODE_INSTANCE_COUNT_KEY,
            (oldVal + statInstanceCount));
      }
      if (propertyValues.size() > 0) {
        for (String propVal : propertyValues) {
          elementVertex.property(JanusGraphSchemaConstant.STATNODE_RANGE_UNIQUE_VALUES_KEY,
              propVal.hashCode());
        }
      }
    }
  }

  /**
   * This function commits the data. before committing, edges statistics data is flushed into
   * database,
   * 
   * @throws Exception
   */
  public void commit() throws Exception {
    if (ConstantParameters.COLLECT_STATISTICS) {
      for (String edgeLabel : edgeInstanceCount.keySet()) {
        long statInstanceCount = edgeInstanceCount.get(edgeLabel);
        long statDomainCount = edgeDomainCount.get(edgeLabel);
        long statRangeCount = edgeRangeCount.get(edgeLabel);

        Vertex elementVertex = getVertex(JanusGraphSchemaConstant.ID_PROPERTY, edgeLabel);
        if (statInstanceCount > 0) {
          Object value =
              getPropertyValue(elementVertex, JanusGraphSchemaConstant.STATNODE_INSTANCE_COUNT_KEY);
          long oldVal;
          if (value == null) {
            oldVal = 0;
          } else {
            oldVal = (long) value;
          }
          addProperty(elementVertex, JanusGraphSchemaConstant.STATNODE_INSTANCE_COUNT_KEY,
              (oldVal + statInstanceCount));
        }
        if (statDomainCount > 0) {
          Object value =
              getPropertyValue(elementVertex, JanusGraphSchemaConstant.STATNODE_DOMAIN_COUNT_KEY);
          long oldVal;
          if (value == null) {
            oldVal = 0;
          } else {
            oldVal = (long) value;
          }
          addProperty(elementVertex, JanusGraphSchemaConstant.STATNODE_DOMAIN_COUNT_KEY,
              (oldVal + statDomainCount));
        }
        if (statRangeCount > 0) {
          Object value =
              getPropertyValue(elementVertex, JanusGraphSchemaConstant.STATNODE_RANGE_COUNT_KEY);
          long oldVal;
          if (value == null) {
            oldVal = 0;
          } else {
            oldVal = (long) value;
          }
          addProperty(elementVertex, JanusGraphSchemaConstant.STATNODE_RANGE_COUNT_KEY,
              (oldVal + statRangeCount));
        }
      }
      edgeInstanceCount.clear();
      edgeDomainCount.clear();
      edgeRangeCount.clear();
    }
    typeNodes.clear();
    if (connectionMode == ConnectionMode.Direct) {
      g.tx().commit();
    } else if (connectionMode == ConnectionMode.Server) {
      g.close();
    }
    System.gc();
  }
}
