
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

import java.io.IOException;
import java.io.InputStream;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.JanusGraphManagement.IndexBuilder;
import org.janusgraph.core.schema.Mapping;
import org.janusgraph.core.schema.SchemaAction;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.graphdb.database.management.ManagementSystem;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.ibm.research.ergs.ingestion.forwardchaining.GraphDBForwardChainingLoad;
import com.ibm.research.ergs.ingestion.forwardchaining.JFactOntologyProcessor;
import com.ibm.research.ergs.ingestion.forwardchaining.OntologyData;
import com.ibm.research.ergs.ingestion.loader.InputParameters;
import com.ibm.research.ergs.ingestion.naiveload.GraphDBNaiveLoad;

/**
 * This is JanusGraphConnection interface implementation class for direct mode loading.
 * 
 * @author Sumit Neelam (sumit.neelam@in.ibm.com)
 *
 */
public class JanusGraphConnectionDirectMode implements JanusGraphConnection {

  private static final Logger logger =
      LoggerFactory.getLogger(JanusGraphConnectionDirectMode.class);

  JanusGraph janusGraph;
  OntologyData ontologyData;

  /**
   * Constructs {@link JanusGraphConnectionDirectMode}
   * 
   * @param janusGraph
   * @param tboxStream
   */
  public JanusGraphConnectionDirectMode(JanusGraph janusGraph) {
    this.janusGraph = janusGraph;
  }

  @Override
  public void createOntologyBaseGraph(InputStream tboxStream) {
    if (InputParameters.ENABLE_FORWARD_CHAINING) {
      if (tboxStream != null) {
        try {
          JFactOntologyProcessor jFactOntologyProcessor = new JFactOntologyProcessor();
          OntologyData ontologyData = jFactOntologyProcessor.processOntology(tboxStream);

          GraphDBForwardChainingLoad graphDBForwardChainingLoad =
              new GraphDBForwardChainingLoad(getTraversal(), ConnectionMode.Direct, ontologyData);
          graphDBForwardChainingLoad.createInitialFCGraph(getSchemaCreator());
          graphDBForwardChainingLoad.writeOntologyData(getSchemaCreator());
          graphDBForwardChainingLoad.commit();

        } catch (OWLOntologyCreationException e) {
          logger.error("Exception in processing ontology.", e);
        } catch (Exception e) {
          logger.error("Exception in closing connection.", e);
        }
      } else {
        logger.error("input.enableforwardchaining is enabled without specifying tbox");
      }
    } else {
      try {
        GraphDBNaiveLoad graphDBNaiveLoad =
            new GraphDBNaiveLoad(getTraversal(), ConnectionMode.Direct);
        graphDBNaiveLoad.storeOntologyAsProperty(tboxStream);
        graphDBNaiveLoad.commit();
      } catch (IOException e) {
        logger.error("Exception in storing ontology:", e);
      } catch (Exception e) {
        logger.error("Exception in closing connection:", e);
      }
    }
  }

  @Override
  public SchemaCreator getSchemaCreator() {
    SchemaCreator schemaCreator = new SchemaCreatorDirectMode(janusGraph.openManagement());
    return (schemaCreator);
  }

  @Override
  public GraphDBLoad getLoader() {
    if (InputParameters.ENABLE_FORWARD_CHAINING) {
      if (ontologyData == null) {
        GraphDBForwardChainingLoad graphDBForwardChainingLoad =
            new GraphDBForwardChainingLoad(getTraversal(), ConnectionMode.Direct, null);
        ontologyData = graphDBForwardChainingLoad.getOntologyData();
      }
      return (new GraphDBForwardChainingLoad(getTraversal(), ConnectionMode.Direct, ontologyData));
    } else {
      return (new GraphDBNaiveLoad(getTraversal(), ConnectionMode.Direct));
    }
  }

  @Override
  public void buildAllPropertyExternalIndex(String indexName) {
    try {
      logger.info("Creating All property index");
      GraphTraversalSource g = janusGraph.traversal();
      Vertex propVertex = g.V().has(JanusGraphSchemaConstant.ID_PROPERTY, "propertyKeys").next();
      Vertex conflictVertex = g.V().has(JanusGraphSchemaConstant.ID_PROPERTY, "conflicts").next();
      Vertex allPropVertex = g.V().has(JanusGraphSchemaConstant.ID_PROPERTY, "allPropIndex").next();
      HashSet<String> propSet = new HashSet<String>(propVertex.keys());
      propSet.addAll(conflictVertex.keys());

      /* delete id,oid property, it will not be part of all property index */
      propSet.remove(JanusGraphSchemaConstant.ID_PROPERTY);
      propSet.remove(JanusGraphSchemaConstant.ORG_ID_PROPERTY);

      logger.info("Total properties: " + propSet.size());

      JanusGraphManagement mgmt = janusGraph.openManagement();

      IndexBuilder indexBuilder = mgmt.buildIndex(indexName, Vertex.class);
      for (String property : propSet) {
        PropertyKey p = mgmt.getPropertyKey(property + "_prop");
        indexBuilder.addKey(p, Mapping.TEXT.asParameter());
        allPropVertex.property(property, indexName);
      }
      indexBuilder.buildMixedIndex("search");
      mgmt.commit();
      janusGraph.tx().commit();

      ManagementSystem.awaitGraphIndexStatus(janusGraph, indexName).status(SchemaStatus.REGISTERED)
          .timeout(-1, ChronoUnit.HOURS).call();

      mgmt = janusGraph.openManagement();
      JanusGraphIndex i = mgmt.getGraphIndex(indexName);
      mgmt.updateIndex(i, SchemaAction.REINDEX);
      mgmt.commit();

      ManagementSystem.awaitGraphIndexStatus(janusGraph, indexName).status(SchemaStatus.ENABLED)
          .timeout(-1, ChronoUnit.HOURS).call();
      logger.info("Reindexing done.");

      g.close();
    } catch (Exception e) {
      logger.error("Exception: ", e);
      logger.error("Skipping all property index creation");
    }
  }

  /**
   * returns GraphTraversalSource object for accessing and modification of graph
   * 
   * @return
   */
  private GraphTraversalSource getTraversal() {
    GraphTraversalSource g = janusGraph.newTransaction().traversal();
    return (g);
  }
}
