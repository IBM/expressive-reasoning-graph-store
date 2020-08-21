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
package com.ibm.research.ergs.query.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.attribute.Text;

/**
 * This class is responsible for calculating cost for different expressions in a query
 *
 * @author Udit Sharma/Sumit Neelam
 *
 */
public class CostModel {
  private JanusGraph graph;
  private Client client;
  private String graphName;
  private String traversalName;
  private GraphTraversalSource traversalSource;

  private static final long CARDINALITY_MAX = Long.MAX_VALUE / 1000;
  private static final long CARDINALITY_MIN = 1;

  /**
   * Constructs {@link CostModel}
   *
   * @param graph graph instance
   * @param graphName name of graph in DB
   */
  public CostModel(JanusGraph graph, String graphName) {
    this.graph = graph;
    this.graphName = graphName;
    this.traversalSource = graph.traversal();
  }

  /**
   * Constructs {@link CostModel}
   *
   * @param client {@link Client} for graph
   * @param graphName name of graph in DB
   * @param traversalName name of traversal in DB
   */
  public CostModel(Client client, String graphName, String traversalName) {
    this.client = client;
    this.graphName = graphName;
    this.traversalName = traversalName;
    this.traversalSource = AnonymousTraversalSource.traversal()
        .withRemote(DriverRemoteConnection.using(this.client.getCluster(), this.traversalName));
  }

  public long getMaxCardinality() {
    return CARDINALITY_MAX;
  }

  public long getMinCardinality() {
    return CARDINALITY_MIN;
  }

  private String appendEdgePrefix(String edgeLabel) {
    return edgeLabel + "_edge";
  }

  private String appendPropertyPrefix(String propertyKey) {
    return propertyKey + "_prop";
  }

  /**
   *
   * @param edgeLabel string containing label present on edge
   * @return Cardinality of relationship when domain is fixed but not provided
   */
  public long getRelationshipRangeCardinalityForFixedDomain(String edgeLabel) {
    // handles request of the form
    // (<domainVertexID> <edgeLabel> ?o/<rangeVertexID>)

    // If edgeLabel is variable
    if (edgeLabel.equals("null")) {
      return CARDINALITY_MAX;
    }
    try {
      Map<Object, Object> result = traversalSource.V().has("id", edgeLabel).valueMap().next();
      // Number of instances of edge
      Object instanceCountObj = result.get("instancecount");
      // Number of distinct source nodes associated with the edge
      Object domainCardinalityObj = result.get("domaincount");
      if (instanceCountObj == null || domainCardinalityObj == null) {
        return CARDINALITY_MIN;
      }
      long instanceCount = ((ArrayList<Long>) instanceCountObj).get(0);
      long domainCardinality = ((ArrayList<Long>) domainCardinalityObj).get(0);
      // get the ratio to obtain actual cardinality
      return (long) Math.ceil(instanceCount / domainCardinality);
    } catch (Exception e) {
      return CARDINALITY_MIN;
    }
  }

  /**
   *
   * @param edgeLabel label present on edge
   * @param domainVertexID source node for the given edge
   * @return Cardinality of relationship when domain is fixed and provided
   */
  public long getRelationshipRangeCardinalityForFixedDomain(String edgeLabel,
      String domainVertexID) {
    // handles request of the form
    // (<domainVertexID> ?p/<edgeLabel> ?o/<rangeVertexID>)

    try {
      // If edge label is not specified get count of all the outgoing edges
      if (edgeLabel.equals("null")) {
        return traversalSource.V().has("id", domainVertexID).outE().count().next();
      } else {
        edgeLabel = appendEdgePrefix(edgeLabel);
        return traversalSource.V().has("id", domainVertexID).outE(edgeLabel).count().next();
      }
    } catch (Exception e) {
      return CARDINALITY_MIN;
    }
  }

  /**
   *
   * @param edgeLabel label present on edge
   * @param domainVertexIdList {@link Collection} of source node for the given edge
   * @return Cardinality of relationship when multiple domain nodes are provided
   */
  public long getRelationshipRangeCardinalityForFixedDomain(String edgeLabel,
      Collection<Object> domainVertexIdList) {
    // handles request of the form
    // (<domainVertexID> ?p/<edgeLabel> ?o/<rangeVertexID>
    // FILTER ?domainVertexId in domainVertexIdList)

    try {
      // If edge label is not specified get count of all the outgoing edges
      if (edgeLabel.equals("null")) {
        return traversalSource.V().has("id", P.within(domainVertexIdList)).outE().count().next();
      } else {
        edgeLabel = appendEdgePrefix(edgeLabel);
        return traversalSource.V().has("id", P.within(domainVertexIdList)).outE(edgeLabel).count()
            .next();
      }
    } catch (Exception e) {
      return CARDINALITY_MIN;
    }
  }

  /**
   *
   * @param edgeLabel string containing label present on edge
   * @return Cardinality of relationship when range is fixed but not provided
   */
  public long getRelationshipDomainCardinalityForFixedRange(String edgeLabel) {
    // handles request of the form
    // (?s/<domainVertexID> <edgeLabel> <rangeVertexID>)

    if (edgeLabel.equals("null")) {
      return CARDINALITY_MAX;
    }
    try {
      Map<Object, Object> result = traversalSource.V().has("id", edgeLabel).valueMap().next();
      // Number of instances of edge
      Object instanceCountObj = result.get("instancecount");
      // Number of distinct destination nodes associated with the edge
      Object rangeCardinalityObj = result.get("rangecount");
      if (instanceCountObj == null || rangeCardinalityObj == null) {
        return CARDINALITY_MIN;
      }
      long instanceCount = ((ArrayList<Long>) instanceCountObj).get(0);
      long rangeCardinality = ((ArrayList<Long>) rangeCardinalityObj).get(0);
      // get the ratio to obtain actual cardinality
      return (long) Math.ceil(instanceCount / rangeCardinality);
    } catch (Exception e) {
      return CARDINALITY_MIN;
    }
  }

  /**
   *
   * @param edgeLabel label present on edge
   * @param rangeVertexID destination node for the given edge
   * @return Cardinality of relationship when range is fixed and provided
   */
  public long getRelationshipDomainCardinalityForFixedRange(String edgeLabel,
      String rangeVertexID) {
    // handles request of the form
    // (?s/<domainVertexID> ?p/<edgeLabel> <rangeVertexID>)
    try {
      // If edge label is not specified get count of all the incoming edges
      if (edgeLabel.equals("null")) {
        return traversalSource.V().has("id", rangeVertexID).inE().count().next();
      } else {
        edgeLabel = appendEdgePrefix(edgeLabel);
        return traversalSource.V().has("id", rangeVertexID).inE(edgeLabel).count().next();
      }
    } catch (Exception e) {
      return CARDINALITY_MIN;
    }
  }

  // (?s/<domainVertexID> ?p/<edgeLabel> <rangeVertexID> FILTER ?rangeVertexID
  // in
  // rangeVertexIDList)
  /**
   *
   * @param edgeLabel label present on edge
   * @param rangeVertexIdList {@link Collection} of destination node for the given edge
   * @return Cardinality of relationship when multiple range nodes are provided
   */
  public long getRelationshipDomainCardinalityForFixedRange(String edgeLabel,
      Collection<Object> rangeVertexIdList) {
    // handles request of the form
    // (?s/<domainVertexID> ?p/<edgeLabel> <rangeVertexID>
    // FILTER ?rangeVertexID in rangeVertexIDList)
    try {
      // If edge label is not specified get count of all the incoming edges
      if (edgeLabel.equals("null")) {
        return traversalSource.V().has("id", P.within(rangeVertexIdList)).inE().count().next();
      } else {
        edgeLabel = appendEdgePrefix(edgeLabel);
        return traversalSource.V().has("id", P.within(rangeVertexIdList)).inE(edgeLabel).count()
            .next();
      }
    } catch (Exception e) {
      return CARDINALITY_MIN;
    }
  }

  /**
   *
   * @param edgeLabel string containing label present on edge
   * @return Cardinality of relationship when range is not fixed
   */
  public long getRelationshipDomainCardinality(String edgeLabel) {
    // handles request of the form
    // (?s <edgeLabel> ?o)
    try {
      // Number of distinct source nodes associated with the edge
      GraphTraversal<Vertex, Long> g =
          traversalSource.V().has("id", edgeLabel).values("domaincount");
      return g.next();
    } catch (Exception e) {
      return CARDINALITY_MIN;
    }
  }

  public long getPropertyCardinality(String propertyKey) {
    // handles request of the form
    // (?s <propertyKey> ?o)
    try {
      // Find the index name
      GraphTraversal<Vertex, String> g =
          traversalSource.V().has("id", "indexProp").values(propertyKey);
      if (!g.hasNext()) {
        propertyKey = appendPropertyPrefix(propertyKey);
        return traversalSource.V().has("label_prop", propertyKey).count().next();
      } else {
        String indexName = g.next();
        propertyKey = appendPropertyPrefix(propertyKey);
        return getPropertyCardinality(propertyKey, indexName);
      }
    } catch (Exception e) {
      return CARDINALITY_MIN;
    }
  }

  /**
   *
   * @param propertyKey property key
   * @param indexName name of index used to index property key
   * @return cardinality of given property
   */
  private long getPropertyCardinality(String propertyKey, String indexName) {
    String directIndexQuery = "v.\\\"" + propertyKey + "\\\":*";
    // server mode
    if (graph == null) {
      // get the count of vertices having data properties with given key
      String query = graphName + ".indexQuery(\"" + indexName + "\" , \"" + directIndexQuery
          + "\").vertexTotals()";
      ResultSet rs = client.submit(query);
      Iterator<Result> i = rs.iterator();
      if (i.hasNext()) {
        return (i.next().getLong());
      } else {
        return CARDINALITY_MIN;
      }
    }
    // local mode
    else {
      // get the count of vertices having data properties with given key
      return graph.indexQuery(indexName, directIndexQuery).vertexTotals();
    }
  }

  /**
   *
   * @param propertyKey property key
   * @param regex regular expression
   * @return Cardinality of nodes where property value for given propertyKey matches given regex
   */
  public long getPropertyCardinalityForRegex(String propertyKey, String regex) {
    // handles request of the form
    // (?s <propertyKey> "regex")
    return getPropertyCardinalityForPredicate(propertyKey, Text.textRegex(regex));
  }

  /**
   *
   * @param propertyKey property key
   * @param predicate {@link P} filter
   * @return Cardinality of nodes where property value for given propertyKey matches given predicate
   */
  public long getPropertyCardinalityForPredicate(String propertyKey, P predicate) {
    try {
      GraphTraversal<Vertex, String> g =
          traversalSource.V().has("id", "indexProp").values(propertyKey);
      // if property is not indexed
      if (!g.hasNext()) {
        return CARDINALITY_MAX;
      }
      // if property is indexed get the actual cardinality
      else {
        propertyKey = appendPropertyPrefix(propertyKey);
        return traversalSource.V().has(propertyKey, predicate).count().next();
      }
    } catch (Exception e) {
      return CARDINALITY_MIN;
    }
  }

  /**
   *
   * @param propertyKey property key
   * @param propertyValue property value
   * @return Cardinality of nodes where property value for given propertyKey matches given
   *         propertyValue
   */
  public long getPropertyCardinalityForExactMatch(String propertyKey, String propertyValue) {
    // handles request of the form
    // (?s <propertyKey> "propertyValue")
    return getPropertyCardinalityForPredicate(propertyKey, P.eq(propertyValue));
  }

}
