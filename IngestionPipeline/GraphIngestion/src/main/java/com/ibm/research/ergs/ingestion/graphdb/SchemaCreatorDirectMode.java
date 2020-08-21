
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

import java.util.Date;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.Multiplicity;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.Mapping;
import org.janusgraph.core.schema.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SchemaCreator implementation class for direct mode execution.
 * 
 * @author Sumit Neelam (sumit.neelam@in.ibm.com)
 *
 */
public class SchemaCreatorDirectMode implements SchemaCreator {

  private static final Logger logger = LoggerFactory.getLogger(SchemaCreatorDirectMode.class);

  JanusGraphManagement mgmt;

  /**
   * Constructs {@link SchemaCreatorDirectMode}
   * 
   * @param mgmt
   */
  public SchemaCreatorDirectMode(JanusGraphManagement mgmt) {
    this.mgmt = mgmt;
  }

  @Override
  public void createProperty(String propertyName, String propertyType, boolean multivalued,
      boolean isSet) {
    if (!mgmt.containsPropertyKey(propertyName)) {
      Cardinality cardinalityType;
      if (isSet) {
        cardinalityType = Cardinality.SET;
      } else {
        cardinalityType = Cardinality.LIST;
      }
      if (propertyType.equalsIgnoreCase("string")) {
        if (multivalued) {
          mgmt.makePropertyKey(propertyName).dataType(String.class).cardinality(cardinalityType)
              .make();
        } else {
          mgmt.makePropertyKey(propertyName).dataType(String.class).make();
        }
      } else if (propertyType.equalsIgnoreCase("integer")
          || propertyType.equalsIgnoreCase("negativeInteger")
          || propertyType.equalsIgnoreCase("nonNegativeInteger")
          || propertyType.equalsIgnoreCase("nonPositiveInteger")
          || propertyType.equalsIgnoreCase("positiveInteger")) {
        if (multivalued) {
          mgmt.makePropertyKey(propertyName).dataType(Integer.class).cardinality(cardinalityType)
              .make();
        } else {
          mgmt.makePropertyKey(propertyName).dataType(Integer.class).make();
        }
      } else if (propertyType.equalsIgnoreCase("decimal")
          || propertyType.equalsIgnoreCase("float")) {
        if (multivalued) {
          mgmt.makePropertyKey(propertyName).dataType(Float.class).cardinality(cardinalityType)
              .make();
        } else {
          mgmt.makePropertyKey(propertyName).dataType(Float.class).make();
        }
      } else if (propertyType.equalsIgnoreCase("double")) {
        if (multivalued) {
          mgmt.makePropertyKey(propertyName).dataType(Double.class).cardinality(cardinalityType)
              .make();
        } else {
          mgmt.makePropertyKey(propertyName).dataType(Double.class).make();
        }
      } else if (propertyType.equalsIgnoreCase("boolean")) {
        if (multivalued) {
          mgmt.makePropertyKey(propertyName).dataType(Boolean.class).cardinality(cardinalityType)
              .make();
        } else {
          mgmt.makePropertyKey(propertyName).dataType(Boolean.class).make();
        }
      } else if (propertyType.equalsIgnoreCase("long")) {
        if (multivalued) {
          mgmt.makePropertyKey(propertyName).dataType(Long.class).cardinality(cardinalityType)
              .make();
        } else {
          mgmt.makePropertyKey(propertyName).dataType(Long.class).make();
        }
      } else if (propertyType.equalsIgnoreCase("date") || propertyType.equalsIgnoreCase("gYear")
          || propertyType.equalsIgnoreCase("gYearMonth")) {
        if (multivalued) {
          mgmt.makePropertyKey(propertyName).dataType(Date.class).cardinality(cardinalityType)
              .make();
        } else {
          mgmt.makePropertyKey(propertyName).dataType(Date.class).make();
        }
      } else {
        if (multivalued) {
          mgmt.makePropertyKey(propertyName).dataType(String.class).cardinality(cardinalityType)
              .make();
        } else {
          mgmt.makePropertyKey(propertyName).dataType(String.class).make();
        }
      }
      logger.info("Property created: " + propertyName);
    }
  }

  @Override
  public void createIndexProperty(String propertyName, String propertyType, boolean isUnique,
      boolean multivalued, boolean isSet) {
    if (!mgmt.containsPropertyKey(propertyName)) {
      PropertyKey kee;
      Cardinality cardinalityType;
      if (isSet) {
        cardinalityType = Cardinality.SET;
      } else {
        cardinalityType = Cardinality.LIST;
      }
      if (propertyType.equalsIgnoreCase("string")) {
        if (multivalued) {
          kee = mgmt.makePropertyKey(propertyName).dataType(String.class)
              .cardinality(cardinalityType).make();
        } else {
          kee = mgmt.makePropertyKey(propertyName).dataType(String.class).make();
        }
        if (isUnique) {
          mgmt.buildIndex(propertyName, Vertex.class).addKey(kee).unique().buildCompositeIndex();
        } else {
          mgmt.buildIndex(propertyName, Vertex.class).addKey(kee).buildCompositeIndex();
        }
      } else if (propertyType.equalsIgnoreCase("integer")
          || propertyType.equalsIgnoreCase("negativeInteger")
          || propertyType.equalsIgnoreCase("nonNegativeInteger")
          || propertyType.equalsIgnoreCase("nonPositiveInteger")
          || propertyType.equalsIgnoreCase("positiveInteger")) {
        if (multivalued) {
          kee = mgmt.makePropertyKey(propertyName).dataType(Integer.class)
              .cardinality(cardinalityType).make();
        } else {
          kee = mgmt.makePropertyKey(propertyName).dataType(Integer.class).make();
        }
        if (isUnique) {
          mgmt.buildIndex(propertyName, Vertex.class).addKey(kee).unique().buildCompositeIndex();
        } else {
          mgmt.buildIndex(propertyName, Vertex.class).addKey(kee).buildCompositeIndex();
        }
      } else if (propertyType.equalsIgnoreCase("long")) {
        if (multivalued) {
          kee = mgmt.makePropertyKey(propertyName).dataType(Long.class).cardinality(cardinalityType)
              .make();
        } else {
          kee = mgmt.makePropertyKey(propertyName).dataType(Long.class).make();
        }
        if (isUnique) {
          mgmt.buildIndex(propertyName, Vertex.class).addKey(kee).unique().buildCompositeIndex();
        } else {
          mgmt.buildIndex(propertyName, Vertex.class).addKey(kee).buildCompositeIndex();
        }
      } else if (propertyType.equalsIgnoreCase("date") || propertyType.equalsIgnoreCase("gYear")
          || propertyType.equalsIgnoreCase("gYearMonth")) {
        if (multivalued) {
          kee = mgmt.makePropertyKey(propertyName).dataType(Date.class).cardinality(cardinalityType)
              .make();
        } else {
          kee = mgmt.makePropertyKey(propertyName).dataType(Date.class).make();
        }
        if (isUnique) {
          mgmt.buildIndex(propertyName, Vertex.class).addKey(kee).unique().buildCompositeIndex();
        } else {
          mgmt.buildIndex(propertyName, Vertex.class).addKey(kee).buildCompositeIndex();
        }
      } else {
        logger.error("Property creation failed: " + propertyName
            + ". System has not support indexing of property type: " + propertyType);
      }
      logger.info("Index property created: " + propertyName);
    }
  }

  @Override
  public void createExternalIndexProperty(String propertyName, String indexName,
      String propertyType, boolean multivalued, boolean isSet, String indexType) {
    if (!mgmt.containsPropertyKey(propertyName)) {
      PropertyKey kee;
      Parameter p;
      if (indexType.equalsIgnoreCase("text")) {
        p = Mapping.TEXT.asParameter();
      } else if (indexType.equalsIgnoreCase("string")) {
        p = Mapping.STRING.asParameter();
      } else {
        p = Mapping.TEXTSTRING.asParameter();
      }
      Cardinality cardinalityType;
      if (isSet) {
        cardinalityType = Cardinality.SET;
      } else {
        cardinalityType = Cardinality.LIST;
      }
      if (propertyType.equalsIgnoreCase("string")) {
        if (multivalued) {
          kee = mgmt.makePropertyKey(propertyName).dataType(String.class)
              .cardinality(cardinalityType).make();
        } else {
          kee = mgmt.makePropertyKey(propertyName).dataType(String.class).make();
        }

        mgmt.buildIndex(indexName, Vertex.class).addKey(kee, p).buildMixedIndex("search");
      } else if (propertyType.equalsIgnoreCase("integer")
          || propertyType.equalsIgnoreCase("negativeInteger")
          || propertyType.equalsIgnoreCase("nonNegativeInteger")
          || propertyType.equalsIgnoreCase("nonPositiveInteger")
          || propertyType.equalsIgnoreCase("positiveInteger")) {
        if (multivalued) {
          kee = mgmt.makePropertyKey(propertyName).dataType(Integer.class)
              .cardinality(cardinalityType).make();
        } else {
          kee = mgmt.makePropertyKey(propertyName).dataType(Integer.class).make();
        }

        mgmt.buildIndex(indexName, Vertex.class).addKey(kee).buildMixedIndex("search");
      } else if (propertyType.equalsIgnoreCase("long")) {
        if (multivalued) {
          kee = mgmt.makePropertyKey(propertyName).dataType(Long.class).cardinality(cardinalityType)
              .make();
        } else {
          kee = mgmt.makePropertyKey(propertyName).dataType(Long.class).make();
        }

        mgmt.buildIndex(indexName, Vertex.class).addKey(kee).buildMixedIndex("search");
      } else if (propertyType.equalsIgnoreCase("decimal")
          || propertyType.equalsIgnoreCase("float")) {
        if (multivalued) {
          kee = mgmt.makePropertyKey(propertyName).dataType(Float.class)
              .cardinality(cardinalityType).make();
        } else {
          kee = mgmt.makePropertyKey(propertyName).dataType(Float.class).make();
        }

        mgmt.buildIndex(indexName, Vertex.class).addKey(kee).buildMixedIndex("search");
      } else if (propertyType.equalsIgnoreCase("double")) {
        if (multivalued) {
          kee = mgmt.makePropertyKey(propertyName).dataType(Double.class)
              .cardinality(cardinalityType).make();
        } else {
          kee = mgmt.makePropertyKey(propertyName).dataType(Double.class).make();
        }

        mgmt.buildIndex(indexName, Vertex.class).addKey(kee).buildMixedIndex("search");
      } else if (propertyType.equalsIgnoreCase("date") || propertyType.equalsIgnoreCase("gYear")
          || propertyType.equalsIgnoreCase("gYearMonth")) {
        if (multivalued) {
          kee = mgmt.makePropertyKey(propertyName).dataType(Date.class).cardinality(cardinalityType)
              .make();
        } else {
          kee = mgmt.makePropertyKey(propertyName).dataType(Date.class).make();
        }

        mgmt.buildIndex(indexName, Vertex.class).addKey(kee).buildMixedIndex("search");
      } else {
        logger.error("Property creation failed: " + propertyName
            + ". System has not support external indexing of property type: " + propertyType);
      }
      logger.info("External index property created: " + propertyName);
    }
  }

  @Override
  public void createEdgeIndexProperty(String propertyName, String propertyType, boolean isUnique) {
    if (!mgmt.containsPropertyKey(propertyName)) {
      PropertyKey kee;

      if (propertyType.equalsIgnoreCase("string")) {
        kee = mgmt.makePropertyKey(propertyName).dataType(String.class).make();
        if (isUnique) {
          mgmt.buildIndex(propertyName, Edge.class).addKey(kee).unique().buildCompositeIndex();
        } else {
          mgmt.buildIndex(propertyName, Edge.class).addKey(kee).buildCompositeIndex();
        }
      } else if (propertyType.equalsIgnoreCase("integer")
          || propertyType.equalsIgnoreCase("negativeInteger")
          || propertyType.equalsIgnoreCase("nonNegativeInteger")
          || propertyType.equalsIgnoreCase("nonPositiveInteger")
          || propertyType.equalsIgnoreCase("positiveInteger")) {
        kee = mgmt.makePropertyKey(propertyName).dataType(Integer.class).make();
        if (isUnique) {
          mgmt.buildIndex(propertyName, Edge.class).addKey(kee).unique().buildCompositeIndex();
        } else {
          mgmt.buildIndex(propertyName, Edge.class).addKey(kee).buildCompositeIndex();
        }
      } else if (propertyType.equalsIgnoreCase("long")) {
        kee = mgmt.makePropertyKey(propertyName).dataType(Long.class).make();
        if (isUnique) {
          mgmt.buildIndex(propertyName, Edge.class).addKey(kee).unique().buildCompositeIndex();
        } else {
          mgmt.buildIndex(propertyName, Edge.class).addKey(kee).buildCompositeIndex();
        }
      } else if (propertyType.equalsIgnoreCase("date") || propertyType.equalsIgnoreCase("gYear")
          || propertyType.equalsIgnoreCase("gYearMonth")) {
        kee = mgmt.makePropertyKey(propertyName).dataType(Date.class).make();
        if (isUnique) {
          mgmt.buildIndex(propertyName, Edge.class).addKey(kee).unique().buildCompositeIndex();
        } else {
          mgmt.buildIndex(propertyName, Edge.class).addKey(kee).buildCompositeIndex();
        }
      } else {
        logger.warn("Indexing system has not support for type " + propertyType + " for property:"
            + propertyName);
        kee = mgmt.makePropertyKey(propertyName).dataType(String.class).make();
        if (isUnique) {
          mgmt.buildIndex(propertyName, Edge.class).addKey(kee).unique().buildCompositeIndex();
        } else {
          mgmt.buildIndex(propertyName, Edge.class).addKey(kee).buildCompositeIndex();
        }
      }
      logger.info("Edge index property created: " + propertyName);
    }
  }

  @Override
  public void createEdgeLabel(String edgeLabel) {
    if (!mgmt.containsEdgeLabel(edgeLabel)) {
      mgmt.makeEdgeLabel(edgeLabel).multiplicity(Multiplicity.SIMPLE).make();
      logger.info("Edge Label created: " + edgeLabel);
    }
  }

  @Override
  public void createSimpleEdgeLabel(String edgeLabel) {
    if (!mgmt.containsEdgeLabel(edgeLabel)) {
      mgmt.makeEdgeLabel(edgeLabel).multiplicity(Multiplicity.SIMPLE).make();
      logger.info("Simple Edge Label created: " + edgeLabel);
    }
  }

  public boolean isPropertyExists(String property) {
    if (mgmt.containsPropertyKey(property)) {
      return (true);
    } else {
      return (false);
    }
  }

  public boolean isEdgeLabelExists(String edgeLabel) {
    if (mgmt.containsEdgeLabel(edgeLabel)) {
      return (true);
    } else {
      return (false);
    }
  }

  @Override
  public void commit() {
    mgmt.commit();
  }
}
