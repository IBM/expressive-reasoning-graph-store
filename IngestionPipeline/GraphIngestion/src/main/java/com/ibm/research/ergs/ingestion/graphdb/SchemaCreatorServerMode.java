
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SchemaCreator implementation class for server mode execution.
 * 
 * @author Sumit Neelam (sumit.neelam@in.ibm.com)
 *
 */
public class SchemaCreatorServerMode implements SchemaCreator {

  private static final Logger logger = LoggerFactory.getLogger(SchemaCreatorServerMode.class);

  JanusGraphConnectionServerMode janusGraphServerModeConnection;
  StringBuffer schemaCommands;

  /**
   * Constructs {@link SchemaCreatorServerMode}
   * 
   * @param janusGraphServerModeConnection
   */
  public SchemaCreatorServerMode(JanusGraphConnectionServerMode janusGraphServerModeConnection) {
    this.janusGraphServerModeConnection = janusGraphServerModeConnection;
    schemaCommands = new StringBuffer();
  }

  @Override
  public void createProperty(String propertyName, String propertyType, boolean multivalued,
      boolean isSet) {
    String cardinalityType;
    if (isSet) {
      cardinalityType = "org.janusgraph.core.Cardinality.SET";
    } else {
      cardinalityType = "org.janusgraph.core.Cardinality.LIST";
    }

    schemaCommands.append("if(!mgmt.containsPropertyKey(\"" + propertyName + "\")){\n");
    if (propertyType.equalsIgnoreCase("string")) {
      if (multivalued) {
        schemaCommands.append("kee = mgmt.makePropertyKey('" + propertyName
            + "').dataType(String.class).cardinality(" + cardinalityType + ").make();");
      } else {
        schemaCommands.append(
            "kee = mgmt.makePropertyKey('" + propertyName + "').dataType(String.class).make();");
      }
    } else if (propertyType.equalsIgnoreCase("integer")
        || propertyType.equalsIgnoreCase("negativeInteger")
        || propertyType.equalsIgnoreCase("nonNegativeInteger")
        || propertyType.equalsIgnoreCase("nonPositiveInteger")
        || propertyType.equalsIgnoreCase("positiveInteger")) {
      if (multivalued) {
        schemaCommands.append("kee = mgmt.makePropertyKey('" + propertyName
            + "').dataType(Integer.class).cardinality(" + cardinalityType + ").make();");
      } else {
        schemaCommands.append(
            "kee = mgmt.makePropertyKey('" + propertyName + "').dataType(Integer.class).make();");
      }
    } else if (propertyType.equalsIgnoreCase("decimal") || propertyType.equalsIgnoreCase("float")) {
      if (multivalued) {
        schemaCommands.append("kee = mgmt.makePropertyKey('" + propertyName
            + "').dataType(Float.class).cardinality(" + cardinalityType + ").make();");
      } else {
        schemaCommands.append(
            "kee = mgmt.makePropertyKey('" + propertyName + "').dataType(Float.class).make();");
      }
    } else if (propertyType.equalsIgnoreCase("double")) {
      if (multivalued) {
        schemaCommands.append("kee = mgmt.makePropertyKey('" + propertyName
            + "').dataType(Double.class).cardinality(" + cardinalityType + ").make();");
      } else {
        schemaCommands.append(
            "kee = mgmt.makePropertyKey('" + propertyName + "').dataType(Double.class).make();");
      }
    } else if (propertyType.equalsIgnoreCase("boolean")) {
      if (multivalued) {
        schemaCommands.append("kee = mgmt.makePropertyKey('" + propertyName
            + "').dataType(Boolean.class).cardinality(" + cardinalityType + ").make();");
      } else {
        schemaCommands.append(
            "kee = mgmt.makePropertyKey('" + propertyName + "').dataType(Boolean.class).make();");
      }
    } else if (propertyType.equalsIgnoreCase("long")) {
      if (multivalued) {
        schemaCommands.append("kee = mgmt.makePropertyKey('" + propertyName
            + "').dataType(Long.class).cardinality(" + cardinalityType + ").make();");
      } else {
        schemaCommands.append(
            "kee = mgmt.makePropertyKey('" + propertyName + "').dataType(Long.class).make();");
      }
    } else if (propertyType.equalsIgnoreCase("date") || propertyType.equalsIgnoreCase("gYear")
        || propertyType.equalsIgnoreCase("gYearMonth")) {
      if (multivalued) {
        schemaCommands.append("kee = mgmt.makePropertyKey('" + propertyName
            + "').dataType(Date.class).cardinality(" + cardinalityType + ").make();");
      } else {
        schemaCommands.append(
            "kee = mgmt.makePropertyKey('" + propertyName + "').dataType(Date.class).make();");
      }
    } else {
      if (multivalued) {
        schemaCommands.append("kee = mgmt.makePropertyKey('" + propertyName
            + "').dataType(String.class).cardinality(org.janusgraph.core.Cardinality.LIST).make();");
      } else {
        schemaCommands.append(
            "kee = mgmt.makePropertyKey('" + propertyName + "').dataType(String.class).make();");
      }
    }
    schemaCommands.append("}\n");
    logger.info("Property created: " + propertyName);
    checkSchemaCommandLength();
  }

  @Override
  public void createIndexProperty(String propertyName, String propertyType, boolean isUnique,
      boolean multivalued, boolean isSet) {
    String cardinalityType;
    if (isSet) {
      cardinalityType = "org.janusgraph.core.Cardinality.SET";
    } else {
      cardinalityType = "org.janusgraph.core.Cardinality.LIST";
    }

    schemaCommands.append("if(!mgmt.containsPropertyKey(\"" + propertyName + "\")){\n");

    if (propertyType.equalsIgnoreCase("string")) {
      if (multivalued) {
        schemaCommands.append("kee = mgmt.makePropertyKey('" + propertyName
            + "').dataType(String.class).cardinality(" + cardinalityType + ").make();");
      } else {
        schemaCommands.append(
            "kee = mgmt.makePropertyKey('" + propertyName + "').dataType(String.class).make();");
      }
      if (isUnique) {
        schemaCommands.append("mgmt.buildIndex('" + propertyName
            + "', Vertex.class).addKey(kee).unique().buildCompositeIndex();");
      } else {
        schemaCommands.append("mgmt.buildIndex('" + propertyName
            + "', Vertex.class).addKey(kee).buildCompositeIndex();");
      }
    } else if (propertyType.equalsIgnoreCase("integer")
        || propertyType.equalsIgnoreCase("negativeInteger")
        || propertyType.equalsIgnoreCase("nonNegativeInteger")
        || propertyType.equalsIgnoreCase("nonPositiveInteger")
        || propertyType.equalsIgnoreCase("positiveInteger")) {
      if (multivalued) {
        schemaCommands.append("kee = mgmt.makePropertyKey('" + propertyName
            + "').dataType(Integer.class).cardinality(" + cardinalityType + ").make();");
      } else {
        schemaCommands.append(
            "kee = mgmt.makePropertyKey('" + propertyName + "').dataType(Integer.class).make();");
      }

      if (isUnique) {
        schemaCommands.append("mgmt.buildIndex('" + propertyName
            + "', Vertex.class).addKey(kee).unique().buildCompositeIndex();");
      } else {
        schemaCommands.append("mgmt.buildIndex('" + propertyName
            + "', Vertex.class).addKey(kee).buildCompositeIndex();");
      }
    } else if (propertyType.equalsIgnoreCase("long")) {
      if (multivalued) {
        schemaCommands.append("kee = mgmt.makePropertyKey('" + propertyName
            + "').dataType(Long.class).cardinality(" + cardinalityType + ").make();");
      } else {
        schemaCommands.append(
            "kee = mgmt.makePropertyKey('" + propertyName + "').dataType(Long.class).make();");
      }

      if (isUnique) {
        schemaCommands.append("mgmt.buildIndex('" + propertyName
            + "', Vertex.class).addKey(kee).unique().buildCompositeIndex();");
      } else {
        schemaCommands.append("mgmt.buildIndex('" + propertyName
            + "', Vertex.class).addKey(kee).buildCompositeIndex();");
      }
    } else if (propertyType.equalsIgnoreCase("date") || propertyType.equalsIgnoreCase("gYear")
        || propertyType.equalsIgnoreCase("gYearMonth")) {
      if (multivalued) {
        schemaCommands.append("kee = mgmt.makePropertyKey('" + propertyName
            + "').dataType(Date.class).cardinality(" + cardinalityType + ").make();");
      } else {
        schemaCommands.append(
            "kee = mgmt.makePropertyKey('" + propertyName + "').dataType(Date.class).make();");
      }
      if (isUnique) {
        schemaCommands.append("mgmt.buildIndex('" + propertyName
            + "', Vertex.class).addKey(kee).unique().buildCompositeIndex();");
      } else {
        schemaCommands.append("mgmt.buildIndex('" + propertyName
            + "', Vertex.class).addKey(kee).buildCompositeIndex();");
      }
    } else {
      logger.error("System has not support for non String property");
    }

    schemaCommands.append("}\n");
    logger.info("Index property created: " + propertyName);
    checkSchemaCommandLength();
  }

  @Override
  public void createExternalIndexProperty(String propertyName, String indexName,
      String propertyType, boolean multivalued, boolean isSet, String indexType) {
    String indexTypeString;
    if (indexType.equalsIgnoreCase("text")) {
      indexTypeString = "org.janusgraph.core.schema.Mapping.TEXT.asParameter()";
    } else if (indexType.equalsIgnoreCase("string")) {
      indexTypeString = "org.janusgraph.core.schema.Mapping.STRING.asParameter()";
    } else {
      indexTypeString = "org.janusgraph.core.schema.Mapping.TEXTSTRING.asParameter()";
    }

    String cardinalityType;
    if (isSet) {
      cardinalityType = "org.janusgraph.core.Cardinality.SET";
    } else {
      cardinalityType = "org.janusgraph.core.Cardinality.LIST";
    }

    schemaCommands.append("if(!mgmt.containsPropertyKey(\"" + propertyName + "\")){\n");
    if (propertyType.equalsIgnoreCase("string")) {
      if (multivalued) {
        schemaCommands.append("kee = mgmt.makePropertyKey('" + propertyName
            + "').dataType(String.class).cardinality(" + cardinalityType + ").make();");
      } else {
        schemaCommands.append(
            "kee = mgmt.makePropertyKey('" + propertyName + "').dataType(String.class).make();");
      }

      schemaCommands.append("mgmt.buildIndex('" + indexName + "', Vertex.class).addKey(kee, "
          + indexTypeString + ").buildMixedIndex(\"search\");");
    } else if (propertyType.equalsIgnoreCase("integer")
        || propertyType.equalsIgnoreCase("negativeInteger")
        || propertyType.equalsIgnoreCase("nonNegativeInteger")
        || propertyType.equalsIgnoreCase("nonPositiveInteger")
        || propertyType.equalsIgnoreCase("positiveInteger")) {
      if (multivalued) {
        schemaCommands.append("kee = mgmt.makePropertyKey('" + propertyName
            + "').dataType(Integer.class).cardinality(" + cardinalityType + ").make();");
      } else {
        schemaCommands.append(
            "kee = mgmt.makePropertyKey('" + propertyName + "').dataType(Integer.class).make();");
      }

      schemaCommands.append("mgmt.buildIndex('" + indexName
          + "', Vertex.class).addKey(kee).buildMixedIndex(\"search\");");
    } else if (propertyType.equalsIgnoreCase("long")) {
      if (multivalued) {
        schemaCommands.append("kee = mgmt.makePropertyKey('" + propertyName
            + "').dataType(Long.class).cardinality(" + cardinalityType + ").make();");
      } else {
        schemaCommands.append(
            "kee = mgmt.makePropertyKey('" + propertyName + "').dataType(Long.class).make();");
      }

      schemaCommands.append("mgmt.buildIndex('" + indexName
          + "', Vertex.class).addKey(kee).buildMixedIndex(\"search\");");
    } else if (propertyType.equalsIgnoreCase("decimal") || propertyType.equalsIgnoreCase("float")) {
      if (multivalued) {
        schemaCommands.append("kee = mgmt.makePropertyKey('" + propertyName
            + "').dataType(Float.class).cardinality(" + cardinalityType + ").make();");
      } else {
        schemaCommands.append(
            "kee = mgmt.makePropertyKey('" + propertyName + "').dataType(Long.class).make();");
      }

      schemaCommands.append("mgmt.buildIndex('" + indexName
          + "', Vertex.class).addKey(kee).buildMixedIndex(\"search\");");
    } else if (propertyType.equalsIgnoreCase("double")) {
      if (multivalued) {
        schemaCommands.append("kee = mgmt.makePropertyKey('" + propertyName
            + "').dataType(Double.class).cardinality(" + cardinalityType + ").make();");
      } else {
        schemaCommands.append(
            "kee = mgmt.makePropertyKey('" + propertyName + "').dataType(Double.class).make();");
      }

      schemaCommands.append("mgmt.buildIndex('" + indexName
          + "', Vertex.class).addKey(kee).buildMixedIndex(\"search\");");
    } else if (propertyType.equalsIgnoreCase("date") || propertyType.equalsIgnoreCase("gYear")
        || propertyType.equalsIgnoreCase("gYearMonth")) {
      if (multivalued) {
        schemaCommands.append("kee = mgmt.makePropertyKey('" + propertyName
            + "').dataType(Date.class).cardinality(" + cardinalityType + ").make();");
      } else {
        schemaCommands.append(
            "kee = mgmt.makePropertyKey('" + propertyName + "').dataType(Date.class).make();");
      }

      schemaCommands.append("mgmt.buildIndex('" + indexName
          + "', Vertex.class).addKey(kee).buildMixedIndex(\"search\");");
    } else {
      logger.warn("Indexing system has not support for type " + propertyType + " for property:"
          + propertyName);
    }

    schemaCommands.append("}\n");
    logger.info("Index property created: " + propertyName);
    checkSchemaCommandLength();
  }

  @Override
  public void createEdgeIndexProperty(String propertyName, String propertyType, boolean isUnique) {
    schemaCommands.append("if(!mgmt.containsPropertyKey(\"" + propertyName + "\")){\n");

    if (propertyType.equalsIgnoreCase("string")) {
      schemaCommands.append(
          "kee = mgmt.makePropertyKey('" + propertyName + "').dataType(String.class).make();");
      if (isUnique) {
        schemaCommands.append("mgmt.buildIndex('" + propertyName
            + "', Edge.class).addKey(kee).unique().buildCompositeIndex();");
      } else {
        schemaCommands.append("mgmt.buildIndex('" + propertyName
            + "', Edge.class).addKey(kee).buildCompositeIndex();");
      }
    } else if (propertyType.equalsIgnoreCase("integer")
        || propertyType.equalsIgnoreCase("negativeInteger")
        || propertyType.equalsIgnoreCase("nonNegativeInteger")
        || propertyType.equalsIgnoreCase("nonPositiveInteger")
        || propertyType.equalsIgnoreCase("positiveInteger")) {
      schemaCommands.append(
          "kee = mgmt.makePropertyKey('" + propertyName + "').dataType(Integer.class).make();");
      if (isUnique) {
        schemaCommands.append("mgmt.buildIndex('" + propertyName
            + "', Edge.class).addKey(kee).unique().buildCompositeIndex();");
      } else {
        schemaCommands.append("mgmt.buildIndex('" + propertyName
            + "', Edge.class).addKey(kee).buildCompositeIndex();");
      }
    } else if (propertyType.equalsIgnoreCase("long")) {
      schemaCommands.append(
          "kee = mgmt.makePropertyKey('" + propertyName + "').dataType(Long.class).make();");
      if (isUnique) {
        schemaCommands.append("mgmt.buildIndex('" + propertyName
            + "', Edge.class).addKey(kee).unique().buildCompositeIndex();");
      } else {
        schemaCommands.append("mgmt.buildIndex('" + propertyName
            + "', Edge.class).addKey(kee).buildCompositeIndex();");
      }
    } else if (propertyType.equalsIgnoreCase("date") || propertyType.equalsIgnoreCase("gYear")
        || propertyType.equalsIgnoreCase("gYearMonth")) {
      schemaCommands.append(
          "kee = mgmt.makePropertyKey('" + propertyName + "').dataType(Date.class).make();");
      if (isUnique) {
        schemaCommands.append("mgmt.buildIndex('" + propertyName
            + "', Edge.class).addKey(kee).unique().buildCompositeIndex();");
      } else {
        schemaCommands.append("mgmt.buildIndex('" + propertyName
            + "', Edge.class).addKey(kee).buildCompositeIndex();");
      }
    } else {
      logger.error("System has not support for non String property");
    }
    logger.info("Edge index property created: " + propertyName);
    checkSchemaCommandLength();
  }

  @Override
  public void createEdgeLabel(String edgeLabel) {
    schemaCommands.append("if(!mgmt.containsEdgeLabel(\"" + edgeLabel + "\")){\n");
    schemaCommands.append("mgmt.makeEdgeLabel('" + edgeLabel
        + "').multiplicity(org.janusgraph.core.Multiplicity.SIMPLE).make();");
    schemaCommands.append("}\n");

    logger.info("Edge Label created: " + edgeLabel);
    checkSchemaCommandLength();
  }

  @Override
  public void createSimpleEdgeLabel(String edgeLabel) {
    schemaCommands.append("if(!mgmt.containsEdgeLabel(\"" + edgeLabel + "\")){\n");
    schemaCommands.append("mgmt.makeEdgeLabel('" + edgeLabel
        + "').multiplicity(org.janusgraph.core.Multiplicity.SIMPLE).make();");
    schemaCommands.append("}\n");

    logger.info("Edge Label created: " + edgeLabel);
    checkSchemaCommandLength();
  }

  private void checkSchemaCommandLength() {
    if (schemaCommands.length() > 40000) {
      commit();
    }
  }

  @Override
  public void commit() {
    if (schemaCommands.length() > 0) {
      janusGraphServerModeConnection.executeSchemaCreationStatements(schemaCommands.toString());
      schemaCommands.delete(0, schemaCommands.length());
    }
  }
}
