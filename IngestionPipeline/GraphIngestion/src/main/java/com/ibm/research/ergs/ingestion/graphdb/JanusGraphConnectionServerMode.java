
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
import java.util.HashSet;
import java.util.Iterator;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.ibm.research.ergs.ingestion.forwardchaining.GraphDBForwardChainingLoad;
import com.ibm.research.ergs.ingestion.forwardchaining.JFactOntologyProcessor;
import com.ibm.research.ergs.ingestion.forwardchaining.OntologyData;
import com.ibm.research.ergs.ingestion.loader.InputParameters;
import com.ibm.research.ergs.ingestion.naiveload.GraphDBNaiveLoad;

/**
 * This is JanusGraphConnection interface implementation class for server mode loading.
 * 
 * @author Sumit Neelam (sumit.neelam@in.ibm.com)
 *
 */
public class JanusGraphConnectionServerMode implements JanusGraphConnection {

  private static final Logger logger =
      LoggerFactory.getLogger(JanusGraphConnectionServerMode.class);

  Client client;
  String graphName;
  OntologyData ontologyData;

  /**
   * Constructs {@link JanusGraphConnectionServerMode}
   * 
   * @param client
   * @param graphName
   * @param tboxStream
   */
  public JanusGraphConnectionServerMode(Client client, String graphName) {
    this.client = client;
    this.graphName = graphName;
  }

  @Override
  public void createOntologyBaseGraph(InputStream tboxStream) {
    if (InputParameters.ENABLE_FORWARD_CHAINING) {
      if (tboxStream != null) {
        try {
          JFactOntologyProcessor jFactOntologyProcessor = new JFactOntologyProcessor();
          OntologyData ontologyData = jFactOntologyProcessor.processOntology(tboxStream);

          GraphDBForwardChainingLoad graphDBForwardChainingLoad =
              new GraphDBForwardChainingLoad(getTraversal(), ConnectionMode.Server, ontologyData);
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
            new GraphDBNaiveLoad(getTraversal(), ConnectionMode.Server);
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
    SchemaCreator schemaCreator = new SchemaCreatorServerMode(this);
    return (schemaCreator);
  }

  @Override
  public GraphDBLoad getLoader() {
    if (InputParameters.ENABLE_FORWARD_CHAINING) {
      if (ontologyData == null) {
        GraphDBForwardChainingLoad graphDBForwardChainingLoad =
            new GraphDBForwardChainingLoad(getTraversal(), ConnectionMode.Server, null);
        ontologyData = graphDBForwardChainingLoad.getOntologyData();
      }
      return (new GraphDBForwardChainingLoad(getTraversal(), ConnectionMode.Server, ontologyData));
    } else {
      return (new GraphDBNaiveLoad(getTraversal(), ConnectionMode.Server));
    }
  }

  @Override
  public void buildAllPropertyExternalIndex(String indexName) {
    try {
      logger.info("Creating All property index");
      GraphTraversalSource g = getTraversal();
      Vertex propVertex = g.V().has(JanusGraphSchemaConstant.ID_PROPERTY, "propertyKeys").next();
      Vertex conflictVertex = g.V().has(JanusGraphSchemaConstant.ID_PROPERTY, "conflicts").next();
      Vertex allPropVertex = g.V().has(JanusGraphSchemaConstant.ID_PROPERTY, "allPropIndex").next();
      HashSet<String> propSet = new HashSet<String>(propVertex.keys());
      propSet.addAll(conflictVertex.keys());

      /* delete id,oid property, it will not be part of all property index */
      propSet.remove(JanusGraphSchemaConstant.ID_PROPERTY);
      propSet.remove(JanusGraphSchemaConstant.ORG_ID_PROPERTY);

      logger.info("Total properties: " + propSet.size());

      StringBuffer indexCommands = new StringBuffer();

      indexCommands.append("JanusGraphManagement mgmt = " + graphName + ".openManagement(); ");
      indexCommands.append(
          "IndexBuilder indexBuilder = mgmt.buildIndex('" + indexName + "', Vertex.class); ");
      for (String property : propSet) {
        indexCommands
            .append("PropertyKey " + property + " = mgmt.getPropertyKey('" + property + "_prop');");
        indexCommands.append("indexBuilder.addKey(" + property + ", Mapping.TEXT.asParameter());");
        g.V(allPropVertex).property(property, indexName).next();
      }
      indexCommands.append("indexBuilder.buildMixedIndex('search');");
      indexCommands.append("mgmt.commit();");
      indexCommands.append("ManagementSystem.awaitGraphIndexStatus(" + graphName + ", '" + indexName
          + "').status(SchemaStatus.REGISTERED).timeout(-1, ChronoUnit.HOURS).call(); ");

      indexCommands.append("JanusGraphManagement mgmt = " + graphName + ".openManagement(); ");
      indexCommands.append("JanusGraphIndex i = mgmt.getGraphIndex('" + indexName + "');");
      indexCommands.append("mgmt.updateIndex(i, SchemaAction.REINDEX);");
      indexCommands.append("mgmt.commit();");
      indexCommands.append("ManagementSystem.awaitGraphIndexStatus(" + graphName + ", '" + indexName
          + "').status(SchemaStatus.ENABLED).timeout(-1, ChronoUnit.HOURS).call();");

      ResultSet rs = client.submit(indexCommands.toString());
      Iterator<Result> i = rs.iterator();
      while (i.hasNext()) {
        logger.info(i.next().toString());
      }
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
    String traversalName = graphName + "_traversal";
    GraphTraversalSource g = EmptyGraph.instance().traversal()
        .withRemote(DriverRemoteConnection.using(client.getCluster(), traversalName));
    return (g);
  }

  /**
   * This function executes schema commands
   * 
   * @param schemaCommands
   */
  public void executeSchemaCreationStatements(String schemaCommands) {
    StringBuffer finalSchemaCommands = new StringBuffer();
    finalSchemaCommands.append("JanusGraphManagement mgmt = " + graphName + ".openManagement(); ");
    finalSchemaCommands.append(schemaCommands);
    finalSchemaCommands.append("mgmt.commit();");
    ResultSet rs = client.submit(finalSchemaCommands.toString());
    Iterator<Result> i = rs.iterator();
    while (i.hasNext()) {
      logger.info(i.next().toString());
    }
  }
}
