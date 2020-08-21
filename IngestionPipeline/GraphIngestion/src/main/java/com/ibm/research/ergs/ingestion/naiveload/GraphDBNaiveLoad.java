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
package com.ibm.research.ergs.ingestion.naiveload;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.vocabulary.OWL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.ibm.research.ergs.ingestion.graphdb.ConnectionMode;
import com.ibm.research.ergs.ingestion.graphdb.GraphDBBaseOperations;
import com.ibm.research.ergs.ingestion.graphdb.GraphDBLoad;
import com.ibm.research.ergs.ingestion.graphdb.JanusGraphSchemaConstant;
import com.ibm.research.ergs.ingestion.loader.ConstantParameters;

/**
 * This class loads the RDF data in graph format in naive format. It does not apply any reasoning
 * technique (Forward Chaining) while loading
 * 
 * @author SumitNeelam (sumit.neelam@in.ibm.com)
 */

public class GraphDBNaiveLoad extends GraphDBBaseOperations implements GraphDBLoad {
  private static final Logger logger = LoggerFactory.getLogger(GraphDBNaiveLoad.class);

  /**
   * Constructs {@link GraphDBNaiveLoad}
   * 
   * @param g: graph traversal object for accessing graph.
   * @param connectionMode: direct mode or server mode.
   */
  public GraphDBNaiveLoad(GraphTraversalSource g, ConnectionMode connectionMode) {
    super(g, connectionMode);
  }

  /**
   * This is entry point for writing data into graph. It first creates type edges, then creates
   * vertex properties followed by edges
   */
  @Override
  public void createCompleteNode(String sourceID, HashMap<String, ArrayList<String>> typeMap,
      HashMap<String, ArrayList<Literal>> properties, HashMap<String, ArrayList<String>> edges,
      ArrayList<String> sameAsList, HashMap<String, String> propertyKeys,
      HashMap<String, String> conflictingLabels) throws Exception {
    Vertex source = getVertex(JanusGraphSchemaConstant.ID_PROPERTY, sourceID);
    Iterator<String> i = g.V(source).values("type");
    Set<String> typeSet = new HashSet<String>();
    while (i.hasNext()) {
      typeSet.add(i.next());
    }

    for (String typeString : typeMap.keySet()) {
      long statInstanceCount = 0;
      long statDomainCount = 0;
      long statRangeCount = 0;
      ArrayList<String> typeClasses = typeMap.get(typeString);
      String typeEdgeLabel = typeString + "_edge";

      if (ConstantParameters.COLLECT_STATISTICS) {
        if (typeSet.size() == 0) {
          statDomainCount++;
          addProperty(source, "label_edge", typeEdgeLabel);
        }
      }

      for (String typeClass : typeClasses) {
        if (!typeSet.contains(typeClass)) {
          Vertex typeNode = getTypeNode(typeClass);

          if (ConstantParameters.COLLECT_STATISTICS) {
            boolean hasInEdges = g.V(typeNode).inE(typeEdgeLabel).hasNext();
            if (!hasInEdges) {
              statRangeCount++;
            }
            statInstanceCount++;
          }
          addEdge(source, typeNode, typeEdgeLabel);
          addProperty(source, "type", typeClass);
          typeSet.add(typeClass);
        }
      }
      writeEdgeStatistics(typeString, statInstanceCount, statDomainCount, statRangeCount);
    }
    addVertexProperties(source, properties, propertyKeys, conflictingLabels);

    /* Add owl:sameAs to edges to treat them as other edges. */
    if (sameAsList.size() > 0) {
      edges.put(OWL.SAMEAS.toString(), sameAsList);
    }

    addVertexEdges(source, edges);
  }

  /**
   * This function creates outgoing edges of vertex
   * 
   * @param vertex: source vertex
   * @param edges: outgoing edges of vertex
   */
  private void addVertexEdges(Vertex vertex, HashMap<String, ArrayList<String>> edges) {
    for (String edgeLabel : edges.keySet()) {
      long statInstanceCount = 0;
      long statDomainCount = 0;
      long statRangeCount = 0;
      ArrayList<String> targets = edges.get(edgeLabel);
      String graphEdgeLabel = edgeLabel + "_edge";

      if (ConstantParameters.COLLECT_STATISTICS) {
        boolean hasOutEdges = g.V(vertex).outE(graphEdgeLabel).hasNext();
        /* this is first edge with given label on vertex */
        if (!hasOutEdges) {
          statDomainCount++;
          addProperty(vertex, "label_edge", graphEdgeLabel);
        }
      }

      for (String targetId : targets) {
        Vertex target = getVertex(JanusGraphSchemaConstant.ID_PROPERTY, targetId);

        if (ConstantParameters.COLLECT_STATISTICS) {
          boolean hasInEdges = g.V(target).inE(graphEdgeLabel).hasNext();
          /* this is first edge with given label on vertex */
          if (!hasInEdges) {
            statRangeCount++;
          }
        }
        statInstanceCount++;

        addEdge(vertex, target, graphEdgeLabel);

      }
      writeEdgeStatistics(edgeLabel, statInstanceCount, statDomainCount, statRangeCount);
    }
  }

  public void storeOntologyAsProperty(InputStream tboxStream) throws IOException {
    Vertex ontVertex = getVertex(JanusGraphSchemaConstant.ID_PROPERTY, "tbox");
    if (tboxStream != null) {
      BufferedReader br = new BufferedReader(new InputStreamReader(tboxStream));
      String line;
      StringBuffer sb = new StringBuffer();
      while ((line = br.readLine()) != null) {
        sb.append(line);
      }
      br.close();
      g.V(ontVertex).property("ontology", sb.toString()).next();
    }
  }
}
