
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
package com.ibm.research.ergs.ingestion.loader;

import java.io.InputStream;
import java.util.Properties;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.janusgraph.core.JanusGraph;
import com.ibm.research.ergs.ingestion.graphdb.JanusGraphConnection;
import com.ibm.research.ergs.ingestion.graphdb.JanusGraphConnectionDirectMode;
import com.ibm.research.ergs.ingestion.graphdb.JanusGraphConnectionServerMode;
import com.ibm.research.ergs.ingestion.graphdb.JanusGraphSchemaConstant;
import com.ibm.research.ergs.ingestion.graphdb.SchemaCreator;

/**
 * This class contains function for creating base graph schema.
 * 
 * @author Sumit Neelam (sumit.neelam@in.ibm.com)
 *
 */
public class CreateGraphFoundationSchema {
  JanusGraphConnection janusGraphConnection;
  // SchemaCreator schemaCreator;
  InputStream tboxStream;

  /**
   * Construcs {@link CreateGraphFoundationSchema} for direct load
   * 
   * @param janusGraph
   */
  public CreateGraphFoundationSchema(JanusGraph janusGraph, InputStream tboxStream,
      Properties properties) {
    janusGraphConnection = new JanusGraphConnectionDirectMode(janusGraph);
    this.tboxStream = tboxStream;
    InputParameters.setParemeters(properties, null);
  }

  /**
   * Construcs {@link CreateGraphFoundationSchema} for server based load.
   * 
   * @param client
   * @param graphName
   */
  public CreateGraphFoundationSchema(Client client, String graphName, InputStream tboxStream,
      Properties properties) {
    janusGraphConnection = new JanusGraphConnectionServerMode(client, graphName);
    this.tboxStream = tboxStream;
    InputParameters.setParemeters(properties, null);
  }

  /**
   * creates base graph schema
   */
  public void createGraphFoundationSchema() {
    createGraphBaseSchema(janusGraphConnection.getSchemaCreator());
    janusGraphConnection.createOntologyBaseGraph(tboxStream);
  }

  private void createGraphBaseSchema(SchemaCreator schemaCreator) {
    schemaCreator.createIndexProperty(JanusGraphSchemaConstant.ID_PROPERTY, "string", false, true,
        true);
    schemaCreator.createIndexProperty(JanusGraphSchemaConstant.ORG_ID_PROPERTY, "string", false,
        true, true);

    schemaCreator.createProperty("type", "string", true, true);
    schemaCreator.createProperty("totalExtIndex", "integer", false, false);
    schemaCreator.createIndexProperty("label_prop", "string", false, true, true);
    schemaCreator.createIndexProperty("label_edge", "string", false, true, true);
    schemaCreator.createProperty(JanusGraphSchemaConstant.STATNODE_INSTANCE_COUNT_KEY, "long",
        false, false);
    schemaCreator.createProperty(JanusGraphSchemaConstant.STATNODE_DOMAIN_COUNT_KEY, "long", false,
        false);
    schemaCreator.createProperty(JanusGraphSchemaConstant.STATNODE_RANGE_COUNT_KEY, "long", false,
        false);
    schemaCreator.createProperty(JanusGraphSchemaConstant.STATNODE_RANGE_UNIQUE_VALUES_KEY,
        "string", true, true);
    schemaCreator.createProperty("language", "string", false, false);
    schemaCreator.createProperty("datatype", "string", false, false);
    schemaCreator.createProperty("isInferred", "boolean", false, false);
    schemaCreator.createEdgeLabel("edgeClass");
    schemaCreator.createProperty("ontology", "string", false, false);
    schemaCreator.commit();
  }
}
