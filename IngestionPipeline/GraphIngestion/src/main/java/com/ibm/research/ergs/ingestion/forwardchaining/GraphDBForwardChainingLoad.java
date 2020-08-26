
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
package com.ibm.research.ergs.ingestion.forwardchaining;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.ibm.research.ergs.ingestion.graphdb.ConnectionMode;
import com.ibm.research.ergs.ingestion.graphdb.GraphDBBaseOperations;
import com.ibm.research.ergs.ingestion.graphdb.GraphDBLoad;
import com.ibm.research.ergs.ingestion.graphdb.JanusGraphSchemaConstant;
import com.ibm.research.ergs.ingestion.graphdb.SchemaCreator;
import com.ibm.research.ergs.ingestion.loader.ConstantParameters;

/**
 * This class contains function to load data with forward chaining enabled
 * 
 * @author Sumit Neelam (sumit.neelam@in.ibm.com)
 *
 */
public class GraphDBForwardChainingLoad extends GraphDBBaseOperations implements GraphDBLoad {

  private static final Logger logger = LoggerFactory.getLogger(GraphDBForwardChainingLoad.class);

  OntologyData ontologyData;

  boolean OLD_SCHEMA = true;

  /**
   * Constructs {@link GraphDBForwardChainingLoad}
   * 
   * @param g
   * @param connectionMode
   * @param ontologyData
   */
  public GraphDBForwardChainingLoad(GraphTraversalSource g, ConnectionMode connectionMode,
      OntologyData ontologyData) {
    super(g, connectionMode);
    this.ontologyData = ontologyData;
  }

  /**
   * This function writes schema for FC graph and creates equivalent class vertices and class
   * hierarchy graph vertices for forward chaining.
   */
  public void createInitialFCGraph(SchemaCreator schemaCreator) {
    createAndWriteOntologySchemaSchema(schemaCreator);
    createEquivalentClassVertices(ontologyData.getEquivalentClasses());
    createClassHierarchyVertices(ontologyData.getClassHierarchy());
  }

  /**
   * This function creates schema information for data super properties, object super properties and
   * inverse properties.
   * 
   * @param schemaCreator
   */
  private void createAndWriteOntologySchemaSchema(SchemaCreator schemaCreator) {
    HashMap<String, String> propertyKeys = new HashMap<>();
    HashSet<String> edgeLabels = new HashSet<>();
    HashMap<String, String> conflictingLabels = new HashMap<>();


    HashMap<String, HashSet<String>> dataSuperProperties = ontologyData.getDataSuperProperties();

    for (String property : dataSuperProperties.keySet()) {
      HashSet<String> superProperties = dataSuperProperties.get(property);
      for (String superProperty : superProperties) {
        schemaCreator.createProperty(superProperty + "_prop", "string", true, false);
        schemaCreator.createProperty(superProperty, "string", false, false);

        if (!conflictingLabels.containsKey(superProperty)) {
          if (edgeLabels.contains(superProperties)) {
            edgeLabels.remove(superProperty);
            conflictingLabels.put(superProperty, "string");
          } else {
            propertyKeys.put(superProperty, "string");
          }
        }
      }
    }

    HashMap<String, HashSet<String>> objSuperProperties = ontologyData.getObjectSuperProperties();

    for (String property : objSuperProperties.keySet()) {
      HashSet<String> superProperties = objSuperProperties.get(property);
      for (String superProperty : superProperties) {
        schemaCreator.createEdgeLabel(superProperty + "_edge");
        schemaCreator.createProperty(superProperty, "string", false, false);

        if (!conflictingLabels.containsKey(superProperty)) {
          if (propertyKeys.containsKey(superProperty)) {
            String datatype = propertyKeys.get(superProperty);
            propertyKeys.remove(superProperty);
            conflictingLabels.put(superProperty, datatype);
          } else {
            edgeLabels.add(superProperty);
          }
        }
      }
    }

    HashMap<String, HashSet<String>> inverseProperties = ontologyData.getInverseProperties();

    for (String property : inverseProperties.keySet()) {
      HashSet<String> inversePropertySet = inverseProperties.get(property);
      for (String inverseProperty : inversePropertySet) {
        schemaCreator.createEdgeLabel(inverseProperty + "_edge");
        schemaCreator.createProperty(inverseProperty, "string", false, false);

        if (!conflictingLabels.containsKey(inverseProperty)) {
          if (propertyKeys.containsKey(inverseProperty)) {
            String datatype = propertyKeys.get(inverseProperty);
            propertyKeys.remove(inverseProperty);
            conflictingLabels.put(inverseProperty, datatype);
          } else {
            edgeLabels.add(inverseProperty);
          }
        }
      }
    }
    schemaCreator.createProperty("superClasses", "string", true, true);
    schemaCreator.createEdgeLabel(RDFS.SUBCLASSOF + "_edge");

    schemaCreator.commit();

    HashMap<String, String> cycleNewIndexProp = new HashMap<>();
    HashSet<String> cycleDeletePropertyKeys = new HashSet<>();
    HashSet<String> cycleDeleteEdgeLabels = new HashSet<>();
    writeSchemaInformation(propertyKeys, edgeLabels, conflictingLabels, cycleNewIndexProp,
        cycleDeletePropertyKeys, cycleDeleteEdgeLabels, 0);
  }

  /**
   * It creates combined vertices for EquivalentClass axioms.
   * 
   * @param equivalentClasses
   */
  private void createEquivalentClassVertices(HashMap<String, ArrayList<String>> equivalentClasses) {
    for (String className : equivalentClasses.keySet()) {
      Vertex classVertex = getVertex(JanusGraphSchemaConstant.ID_PROPERTY, className);
      for (String equivalentClass : equivalentClasses.get(className)) {
        g.V(classVertex).property(JanusGraphSchemaConstant.ID_PROPERTY, equivalentClass)
            .property(JanusGraphSchemaConstant.ORG_ID_PROPERTY, equivalentClass).next();
      }
    }
  }

  /**
   * This function creates class hierarchy vertices.
   * 
   * @param superClasses
   */
  private void createClassHierarchyVertices(HashMap<String, ArrayList<String>> superClasses) {
    for (String classType : superClasses.keySet()) {
      Vertex typeVertex;
      GraphTraversal<Vertex, Vertex> t =
          g.V().has(JanusGraphSchemaConstant.ORG_ID_PROPERTY, classType);
      if (t.hasNext()) {
        typeVertex = t.next();
      } else {
        typeVertex = g.addV().property(JanusGraphSchemaConstant.ID_PROPERTY, classType)
            .property(JanusGraphSchemaConstant.ORG_ID_PROPERTY, classType).next();
      }
      for (String superClass : superClasses.get(classType)) {
        Vertex superClassVertex;
        GraphTraversal<Vertex, Vertex> t1 =
            g.V().has(JanusGraphSchemaConstant.ORG_ID_PROPERTY, superClass);
        if (t1.hasNext()) {
          superClassVertex = t1.next();
        } else {
          superClassVertex = g.addV().property(JanusGraphSchemaConstant.ID_PROPERTY, superClass)
              .property(JanusGraphSchemaConstant.ORG_ID_PROPERTY, superClass).next();
        }
        g.V(typeVertex).as("subClassV").V(superClassVertex).addE(RDFS.SUBCLASSOF + "_edge")
            .from("subClassV").next();
        g.V(typeVertex).property("superClasses", superClass).next();
      }
    }
  }

  /**
   * It writes ontology data in the form of graph.
   * 
   * @param ontologyData
   */
  public void writeOntologyData(SchemaCreator schemaCreator) {
    createOntologySchema(ontologyData, schemaCreator);

    HashMap<String, HashSet<String>> objSuperProperties = ontologyData.getObjectSuperProperties();
    Vertex v1 = getVertex(JanusGraphSchemaConstant.ID_PROPERTY, "objSuperProperties");
    for (String property : objSuperProperties.keySet()) {
      for (String propVal : objSuperProperties.get(property)) {
        g.V(v1).property(property, propVal).next();
      }
    }

    HashMap<String, HashSet<String>> dataSuperProperties = ontologyData.getDataSuperProperties();
    Vertex v2 = getVertex(JanusGraphSchemaConstant.ID_PROPERTY, "dataSuperProperties");
    for (String property : dataSuperProperties.keySet()) {
      for (String propVal : dataSuperProperties.get(property)) {
        g.V(v2).property(property, propVal).next();
      }
    }

    HashMap<String, ArrayList<String>> superClasses = ontologyData.getClassHierarchy();
    Vertex v3 = getVertex(JanusGraphSchemaConstant.ID_PROPERTY, "superClasses");
    for (String className : superClasses.keySet()) {
      for (String propVal : superClasses.get(className)) {
        g.V(v3).property(className, propVal).next();
      }
    }

    HashMap<String, String> propertyDomain = ontologyData.getPropertyDomains();
    Vertex v4 = getVertex(JanusGraphSchemaConstant.ID_PROPERTY, "propertyDomain");
    for (String property : propertyDomain.keySet()) {
      g.V(v4).property(property, propertyDomain.get(property)).next();
    }

    HashMap<String, String> propertyRange = ontologyData.getPropertyRanges();
    Vertex v5 = getVertex(JanusGraphSchemaConstant.ID_PROPERTY, "propertyRange");
    for (String property : propertyRange.keySet()) {
      g.V(v5).property(property, propertyRange.get(property)).next();
    }

    Vertex v6 = getVertex(JanusGraphSchemaConstant.ID_PROPERTY, "transitiveProperties");
    for (String propVal : ontologyData.getTransitiveProperties()) {
      g.V(v6).property("transitiveProperties", propVal).next();
    }

    Vertex v7 = getVertex(JanusGraphSchemaConstant.ID_PROPERTY, "symmetricProperties");
    for (String propVal : ontologyData.getSymmetricProperties()) {
      g.V(v7).property("symmetricProperties", propVal).next();
    }

    HashMap<String, HashSet<String>> inverseProperties = ontologyData.getInverseProperties();
    Vertex v8 = getVertex(JanusGraphSchemaConstant.ID_PROPERTY, "inverseProperties");
    for (String property : inverseProperties.keySet()) {
      for (String propVal : inverseProperties.get(property)) {
        g.V(v8).property(property, propVal).next();
      }
    }
  }

  private void createOntologySchema(OntologyData ontologyData, SchemaCreator schemaCreator) {
    HashMap<String, HashSet<String>> objSuperProperties = ontologyData.getObjectSuperProperties();
    for (String property : objSuperProperties.keySet()) {
      schemaCreator.createProperty(property, "string", true, true);
    }

    HashMap<String, HashSet<String>> dataSuperProperties = ontologyData.getDataSuperProperties();
    for (String property : dataSuperProperties.keySet()) {
      schemaCreator.createProperty(property, "string", true, true);
    }

    HashMap<String, ArrayList<String>> superClasses = ontologyData.getClassHierarchy();
    for (String className : superClasses.keySet()) {
      schemaCreator.createProperty(className, "string", true, true);
    }

    HashMap<String, String> propertyDomain = ontologyData.getPropertyDomains();
    for (String property : propertyDomain.keySet()) {
      schemaCreator.createProperty(property, "string", true, true);
    }

    HashMap<String, String> propertyRange = ontologyData.getPropertyRanges();
    for (String property : propertyRange.keySet()) {
      schemaCreator.createProperty(property, "string", true, true);
    }

    schemaCreator.createProperty("transitiveProperties", "string", true, true);
    schemaCreator.createProperty("symmetricProperties", "string", true, true);

    HashMap<String, HashSet<String>> inverseProperties = ontologyData.getInverseProperties();
    for (String property : inverseProperties.keySet()) {
      schemaCreator.createProperty(property, "string", true, true);
    }
    schemaCreator.commit();
  }

  /**
   * This function reads ontology data saved in graph format
   */
  public OntologyData getOntologyData() {
    HashMap<String, HashSet<String>> objSuperProperties = new HashMap<>();
    Vertex v1 = getVertex(JanusGraphSchemaConstant.ID_PROPERTY, "objSuperProperties");
    Map<Object, Object> r1 = g.V(v1).valueMap().next();
    for (Object p : r1.keySet()) {
      String property = (String) p;
      if (!property.equals(JanusGraphSchemaConstant.ID_PROPERTY)
          && !property.equals(JanusGraphSchemaConstant.ORG_ID_PROPERTY)) {
        ArrayList<String> v = (ArrayList<String>) r1.get(p);
        objSuperProperties.put(property, new HashSet<>(v));
      }
    }

    HashMap<String, HashSet<String>> dataSuperProperties = new HashMap<>();
    Vertex v2 = getVertex(JanusGraphSchemaConstant.ID_PROPERTY, "dataSuperProperties");
    Map<Object, Object> r2 = g.V(v2).valueMap().next();
    for (Object p : r2.keySet()) {
      String property = (String) p;
      if (!property.equals(JanusGraphSchemaConstant.ID_PROPERTY)
          && !property.equals(JanusGraphSchemaConstant.ORG_ID_PROPERTY)) {
        ArrayList<String> v = (ArrayList<String>) r2.get(p);
        dataSuperProperties.put(property, new HashSet<>(v));
      }
    }

    HashMap<String, ArrayList<String>> superClasses = new HashMap<>();
    Vertex v3 = getVertex(JanusGraphSchemaConstant.ID_PROPERTY, "superClasses");
    Map<Object, Object> r3 = g.V(v3).valueMap().next();
    for (Object p : r3.keySet()) {
      String className = (String) p;
      if (!className.equals(JanusGraphSchemaConstant.ID_PROPERTY)
          && !className.equals(JanusGraphSchemaConstant.ORG_ID_PROPERTY)) {
        ArrayList<String> v = (ArrayList<String>) r3.get(p);
        superClasses.put(className, v);
      }
    }

    HashMap<String, String> propertyDomain = new HashMap<>();
    Vertex v4 = getVertex(JanusGraphSchemaConstant.ID_PROPERTY, "propertyDomain");
    Map<Object, Object> r4 = g.V(v4).valueMap().next();
    for (Object p : r4.keySet()) {
      String property = (String) p;
      if (!property.equals(JanusGraphSchemaConstant.ID_PROPERTY)
          && !property.equals(JanusGraphSchemaConstant.ORG_ID_PROPERTY)) {
        ArrayList<String> v = (ArrayList<String>) r4.get(p);
        propertyDomain.put(property, v.get(0));
      }
    }

    HashMap<String, String> propertyRange = new HashMap<>();
    Vertex v5 = getVertex(JanusGraphSchemaConstant.ID_PROPERTY, "propertyRange");
    Map<Object, Object> r5 = g.V(v5).valueMap().next();
    for (Object p : r5.keySet()) {
      String property = (String) p;
      if (!property.equals(JanusGraphSchemaConstant.ID_PROPERTY)
          && !property.equals(JanusGraphSchemaConstant.ORG_ID_PROPERTY)) {
        ArrayList<String> v = (ArrayList<String>) r5.get(p);
        propertyRange.put(property, v.get(0));
      }
    }

    HashSet<String> transitiveProperties = new HashSet<>();
    Vertex v6 = getVertex(JanusGraphSchemaConstant.ID_PROPERTY, "transitiveProperties");
    Map<Object, Object> r6 = g.V(v6).valueMap("transitiveProperties").next();
    for (Object p : r6.keySet()) {
      ArrayList<String> v = (ArrayList<String>) r6.get(p);
      transitiveProperties.addAll(v);
    }

    HashSet<String> symmetricProperties = new HashSet<>();
    Vertex v7 = getVertex(JanusGraphSchemaConstant.ID_PROPERTY, "symmetricProperties");
    Map<Object, Object> r7 = g.V(v7).valueMap("symmetricProperties").next();
    for (Object p : r7.keySet()) {
      ArrayList<String> v = (ArrayList<String>) r7.get(p);
      symmetricProperties.addAll(v);
    }

    HashMap<String, HashSet<String>> inverseProperties = new HashMap<>();
    Vertex v8 = getVertex(JanusGraphSchemaConstant.ID_PROPERTY, "inverseProperties");
    Map<Object, Object> r8 = g.V(v8).valueMap().next();
    for (Object p : r8.keySet()) {
      String property = (String) p;
      if (!property.equals(JanusGraphSchemaConstant.ID_PROPERTY)
          && !property.equals(JanusGraphSchemaConstant.ORG_ID_PROPERTY)) {
        ArrayList<String> v = (ArrayList<String>) r8.get(p);
        inverseProperties.put(property, new HashSet<>(v));
      }
    }

    OntologyData ontologyData = new OntologyData();
    ontologyData.setSymmetricProperties(symmetricProperties);
    ontologyData.setTransitiveProperties(transitiveProperties);
    ontologyData.setInverseProperties(inverseProperties);
    ontologyData.setPropertyDomain(propertyDomain);
    ontologyData.setPropertyRange(propertyRange);
    ontologyData.setObjectSuperProperties(objSuperProperties);
    ontologyData.setDataSuperProperties(dataSuperProperties);
    ontologyData.setSuperClasses(superClasses);
    return (ontologyData);
  }

  @Override
  public void createCompleteNode(String sourceID, HashMap<String, ArrayList<String>> typeMap,
      HashMap<String, ArrayList<Literal>> properties, HashMap<String, ArrayList<String>> edges,
      ArrayList<String> sameAsList, HashMap<String, String> propertyKeys,
      HashMap<String, String> conflictingLabels) throws Exception {
    Vertex source;
    if (sameAsList.size() > 0) {
      source = processSameAs(sourceID, sameAsList);
    } else {
      source = getVertex(JanusGraphSchemaConstant.ID_PROPERTY, sourceID);
    }

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
      // String typeEdgeLabel = typeString;

      if (ConstantParameters.COLLECT_STATISTICS) {
        if (typeSet.size() == 0) {
          statDomainCount++;
          // GraphElementOperations.addStringProperty(source, "label_edge", typeEdgeLabel);
          addProperty(source, "label_edge", typeEdgeLabel);
        }
      }

      for (String typeClass : typeClasses) {
        if (!typeSet.contains(typeClass)) {
          Vertex typeNode = getTypeNode(typeClass);

          if (ConstantParameters.COLLECT_STATISTICS) {
            boolean hasInEdges = g.V(typeNode).outE(typeEdgeLabel).hasNext();
            if (!hasInEdges) {
              statRangeCount++;
            }
            statInstanceCount++;
          }
          addProperty(source, "type", typeClass);
          addEdge(source, typeNode, typeEdgeLabel);
          typeSet.add(typeClass);

          Iterator<String> supClassItr = g.V(typeNode).values("superClasses");
          while (supClassItr.hasNext()) {
            String supClass = supClassItr.next();
            if (!typeSet.contains(supClass)) {
              Vertex supClassNode = getTypeNode(supClass);
              addProperty(source, "type", supClass);
              addEdge(source, supClassNode, typeEdgeLabel);
              typeSet.add(supClass);
            }
          }
        }
      }

      writeEdgeStatistics(typeString, statInstanceCount, statDomainCount, statRangeCount);
    }
    addVertexProperties(source, properties, propertyKeys, conflictingLabels);
    addVertexEdges(source, edges);
  }

  @Override
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

      // String graphPropKey = propertyKey;
      String graphPropKey = propertyKey + "_prop";
      addProperty(vertex, "label_prop", graphPropKey);

      for (Literal propLiteral : propLiteralList) {
        String propVal = propLiteral.getLabel();
        try {
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
        } catch (Exception e) {
          logger.error("Exception: " + e);
        }

        // Storing language and datatype as meta property.
        Optional<String> language = propLiteral.getLanguage();
        if (language.isPresent()) {
          g.V(vertex).properties(graphPropKey).property("language", language.get()).next();
        }
        g.V(vertex).properties(graphPropKey)
            .property("datatype", propLiteral.getDatatype().toString()).next();

        addSuperClassDataProperty(vertex, propertyKey, propVal, datatype);

      }
    }
  }

  /**
   * It add super properties values of given propertyKey.
   * 
   * @param vertex
   * @param propertyKey
   * @param propVal
   * @param datatype
   */
  public void addSuperClassDataProperty(Vertex vertex, String propertyKey, String propVal,
      String datatype) {
    HashSet<String> superProps = ontologyData.getDataSuperProperty(propertyKey);
    if (superProps != null) {
      for (String superProp : superProps) {
        String graphPropKey = superProp + "_prop";
        addProperty(vertex, graphPropKey, propVal);
        g.V(vertex).properties(graphPropKey).property("isInferred", true).next();
      }
    }
  }

  /**
   * It add outgoing edges from vertex with forward chaining enabled.
   * 
   * @param vertex
   * @param edges
   * @throws Exception
   */
  private void addVertexEdges(Vertex vertex, HashMap<String, ArrayList<String>> edges)
      throws Exception {
    for (String edgeLabel : edges.keySet()) {
      long statInstanceCount = 0;
      long statDomainCount = 0;
      long statRangeCount = 0;
      ArrayList<String> targets = edges.get(edgeLabel);
      String graphEdgeLabel = edgeLabel + "_edge";
      // String graphEdgeLabel = edgeLabel;

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

        ArrayList<String> operations = ontologyData.getPropertyTypes(edgeLabel);
        createForwardChainingEnabledEdge(vertex, target, edgeLabel, operations, false);

      }
      writeEdgeStatistics(edgeLabel, statInstanceCount, statDomainCount, statRangeCount);
    }
  }

  /**
   * This is main function for creating forward chaining enable edge.
   * 
   * @param source
   * @param target
   * @param edgeLabel
   * @param operations
   * @param isInferred
   */
  private void createForwardChainingEnabledEdge(Vertex source, Vertex target, String edgeLabel,
      ArrayList<String> operations, boolean isInferred) {
    String graphEdgeLabel = edgeLabel + "_edge";
    if (operations.size() == 0) {
      addEdgeFC(source, target, edgeLabel, isInferred);
    }
    for (String operation : operations) {
      if (operation.equals("transitive")) {
        if (!isEdgeExists(source, target, graphEdgeLabel)) {
          HashMap<Vertex, Boolean> sourceConnectionVertices = new HashMap<>();

          sourceConnectionVertices.put(target, isInferred); // TODO
          if (OLD_SCHEMA) {
            GraphTraversal<Vertex, Vertex> t1 = g.V(target).out(graphEdgeLabel);
            while (t1.hasNext()) {
              Vertex v = t1.next();
              sourceConnectionVertices.put(v, true);
            }
          } else {
            GraphTraversal<Vertex, Vertex> t1 = g.V(target).out(graphEdgeLabel).out(graphEdgeLabel);
            while (t1.hasNext()) {
              Vertex v = t1.next();
              sourceConnectionVertices.put(v, true);
            }
          }
          addEdgeForTransitiveRelationship(source, sourceConnectionVertices, edgeLabel);

          ArrayList<String> updatedOps = new ArrayList<>(operations);
          updatedOps.remove("transitive");
          if (updatedOps.size() > 0) {
            for (Vertex v : sourceConnectionVertices.keySet()) {
              createForwardChainingEnabledEdge(source, v, edgeLabel, updatedOps, true);
            }
          }

          /*
           * update inferred type for target vertex. For last (following) set of edge creation, this
           * will be inferred type.
           */
          sourceConnectionVertices.put(target, true);
          if (OLD_SCHEMA) {
            GraphTraversal<Vertex, Vertex> t3 = g.V(source).in(graphEdgeLabel);
            while (t3.hasNext()) {
              Vertex v = t3.next();
              addEdgeForTransitiveRelationship(v, sourceConnectionVertices, edgeLabel);
              if (updatedOps.size() > 0) {
                for (Vertex t : sourceConnectionVertices.keySet()) {
                  createForwardChainingEnabledEdge(v, t, edgeLabel, updatedOps, true);
                }
              }
            }
          } else {
            GraphTraversal<Vertex, Vertex> t3 = g.V(source).in(graphEdgeLabel).in(graphEdgeLabel);

            while (t3.hasNext()) {
              Vertex v = t3.next();
              addEdgeForTransitiveRelationship(v, sourceConnectionVertices, edgeLabel);
              if (updatedOps.size() > 0) {
                for (Vertex t : sourceConnectionVertices.keySet()) {
                  createForwardChainingEnabledEdge(v, t, edgeLabel, updatedOps, true);
                }
              }
            }
          }

        }
      }
      if (operation.equals("symmetric")) {
        addEdgeFC(source, target, edgeLabel, isInferred);
        ArrayList<String> updatedOps = new ArrayList<>(operations);
        updatedOps.remove("transitive");
        updatedOps.remove("symmetric");
        createForwardChainingEnabledEdge(target, source, edgeLabel, updatedOps, true);
      }
      if (operation.equals("inverse")) {
        addEdgeFC(source, target, edgeLabel, isInferred);
        HashSet<String> invProps = ontologyData.getInverseProperty(edgeLabel);
        ArrayList<String> updatedOps = new ArrayList<>(operations);
        updatedOps.remove("transitive");
        updatedOps.remove("symmetric");
        updatedOps.remove("inverse");
        for (String invLabel : invProps) {
          createForwardChainingEnabledEdge(target, source, invLabel, updatedOps, true);
        }
      }
      if (operation.equals("superproperties")) {
        addEdgeFC(source, target, edgeLabel, isInferred);
        HashSet<String> superProps = ontologyData.getObjectSuperProperty(edgeLabel);
        ArrayList<String> updatedOps = new ArrayList<>(operations);
        updatedOps.remove("transitive");
        updatedOps.remove("symmetric");
        updatedOps.remove("inverse");
        updatedOps.remove("superproperties");
        for (String superProp : superProps) {
          ArrayList<String> superPropOperations = ontologyData.getPropertyTypes(superProp);
          createForwardChainingEnabledEdge(source, target, superProp, superPropOperations, true);
          // addEdgeFC(source, target, superProp, isInferred);
        }
      }
    }
  }

  /**
   * Checks source and target are connected with edge of edgeLabel
   * 
   * @param source
   * @param target
   * @param edgeLabel
   * @return
   */
  private boolean isEdgeExists(Vertex source, Vertex target, String edgeLabel) {
    GraphTraversal<Vertex, Vertex> t;
    if (OLD_SCHEMA) {
      t = g.V(source).out(edgeLabel).where(__.is(P.eq(target)));
    } else {
      t = g.V(source).out(edgeLabel).out(edgeLabel).where(__.is(P.eq(target)));
    }
    if (t.hasNext()) {
      return (true);
    } else {
      return (false);
    }
  }

  /**
   * This functions add edge between source and target with adding domain and range information
   * 
   * @param source
   * @param target
   * @param edgeLabel
   * @param isInferred
   * @return
   */
  public Edge addEdgeFC(Vertex source, Vertex target, String edgeLabel, boolean isInferred) {
    String graphEdgeLabel = edgeLabel + "_edge";
    g.V(source).property("label_edge", graphEdgeLabel).next();

    if (OLD_SCHEMA) {
      Edge edge = g.V(source).as("s").V(target).addE(graphEdgeLabel).from("s").next();

      if (isInferred) {
        g.E(edge).property("isInferred", true).next();
      } else {
        g.E(edge).property("isInferred", false).next();
      }

      String domainClass = ontologyData.getPropertyDomain(edgeLabel);
      String rangeClass = ontologyData.getPropertyRange(edgeLabel);

      if (domainClass != null) {
        connectTypeNode(source, domainClass, RDF.TYPE.toString() + "_edge");
      }
      if (rangeClass != null) {
        connectTypeNode(target, rangeClass, RDF.TYPE.toString() + "_edge");
      }
      return (edge);
    } else {
      GraphTraversal<Vertex, Vertex> traversal = g.V(source).out(graphEdgeLabel);
      Vertex edgeVertex;
      if (traversal.hasNext()) {
        edgeVertex = traversal.next();
      } else {
        edgeVertex = g.addV().property(JanusGraphSchemaConstant.ORG_ID_PROPERTY, graphEdgeLabel)
            .property(JanusGraphSchemaConstant.ID_PROPERTY, graphEdgeLabel).next();

        Vertex edgeTypeVertex = getVertex(JanusGraphSchemaConstant.ORG_ID_PROPERTY, edgeLabel);
        addEdge(edgeVertex, edgeTypeVertex, "edgeClass");

        addEdge(source, edgeVertex, graphEdgeLabel);
      }
      Edge secondEdge = addEdge(edgeVertex, target, graphEdgeLabel);

      if (isInferred) {
        g.E(secondEdge).property("isInferred", true).next();
      }

      String domainClass = ontologyData.getPropertyDomain(edgeLabel);
      String rangeClass = ontologyData.getPropertyDomain(edgeLabel);

      if (domainClass != null) {
        connectTypeNode(source, domainClass, RDF.TYPE.toString() + "_edge");
      }
      if (rangeClass != null) {
        connectTypeNode(target, rangeClass, RDF.TYPE.toString() + "_edge");
      }

      return (secondEdge);
    }
  }

  /**
   * This function adds transitive relations from source to set of targetVertices
   * 
   * @param source
   * @param targetVertices: map of target vertices <targer, isInferred>
   * @param edgeLabel
   */
  private void addEdgeForTransitiveRelationship(Vertex source,
      HashMap<Vertex, Boolean> targetVertices, String edgeLabel) {
    String graphEdgeLabel = edgeLabel + "_edge";
    // Following line is commented b'coz source vertex property 'label_edge' has graphEdgeLabel
    // already.
    // Due to it is transitive property.

    // GraphElementOperations.addStringProperty(source, "label_edge", graphEdgeLabel);
    if (OLD_SCHEMA) {
      for (Vertex targetVertex : targetVertices.keySet()) {
        Edge edge = addEdge(source, targetVertex, graphEdgeLabel);
        boolean isInferred = targetVertices.get(targetVertex);
        if (isInferred) {
          g.E(edge).property("isInferred", true);
        } else {
          g.E(edge).property("isInferred", false);
        }
      }

      String domainClass = ontologyData.getPropertyDomain(edgeLabel);
      if (domainClass != null) {
        connectTypeNode(source, domainClass, RDF.TYPE.toString() + "_edge");
      }
      String rangeClass = ontologyData.getPropertyDomain(edgeLabel);
      if (rangeClass != null) {
        for (Vertex targetVertex : targetVertices.keySet()) {
          connectTypeNode(targetVertex, rangeClass, RDF.TYPE.toString() + "_edge");
        }
      }
    } else {
      GraphTraversal<Vertex, Vertex> traversal = g.V(source).out(graphEdgeLabel);
      Vertex edgeVertex;
      if (traversal.hasNext()) {
        edgeVertex = traversal.next();
      } else {
        edgeVertex = g.addV().property(JanusGraphSchemaConstant.ORG_ID_PROPERTY, graphEdgeLabel)
            .property(JanusGraphSchemaConstant.ID_PROPERTY, graphEdgeLabel).next();

        Vertex edgeTypeVertex = getVertex(JanusGraphSchemaConstant.ORG_ID_PROPERTY, edgeLabel);
        addEdge(edgeVertex, edgeTypeVertex, "edgeClass");

        addEdge(source, edgeVertex, graphEdgeLabel);
      }

      for (Vertex targetVertex : targetVertices.keySet()) {
        Edge secondEdge = addEdge(edgeVertex, targetVertex, graphEdgeLabel);

        boolean isInferred = targetVertices.get(targetVertex);
        if (isInferred) {
          g.E(secondEdge).property("isInferred", true);
        }
      }

      String domainClass = ontologyData.getPropertyDomain(edgeLabel);
      if (domainClass != null) {
        connectTypeNode(source, domainClass, RDF.TYPE.toString() + "_edge");
      }
      String rangeClass = ontologyData.getPropertyDomain(edgeLabel);
      if (rangeClass != null) {
        for (Vertex targetVertex : targetVertices.keySet()) {
          connectTypeNode(targetVertex, rangeClass, RDF.TYPE.toString() + "_edge");
        }
      }

    }
  }

  /**
   * This function connects source node to type node with typeNodeId
   * 
   * @param source
   * @param typeNodeID
   * @param edgeLabel
   */
  private void connectTypeNode(Vertex source, String typeNodeID, String edgeLabel) {
    GraphTraversal<Vertex, Object> t = g.V(source).values("type");
    Set<String> typeSet = new HashSet<String>();
    while (t.hasNext()) {
      typeSet.add((String) t.next());
    }
    if (!typeSet.contains(typeNodeID)) {
      Vertex typeNode = getTypeNode(typeNodeID);
      addEdge(source, typeNode, edgeLabel);
      addProperty(source, "type", typeNodeID);
      typeSet.add(typeNodeID);

      Iterator<String> supClassItr = g.V(typeNode).values("superClasses");
      while (supClassItr.hasNext()) {
        String supClass = supClassItr.next();
        if (!typeSet.contains(supClass)) {
          Vertex supClassNode = getTypeNode(supClass);
          addProperty(source, "type", supClass);
          addEdge(source, supClassNode, edgeLabel);
          typeSet.add(supClass);
        }
      }
    }
  }

  /**
   * This function loads data for owl:sameAs for sourceID vertex.
   * 
   * @param sourceID
   * @param sameAsList: list contains vertex connected to sourceID with owl:sameAs
   * @return
   */
  private Vertex processSameAs(String sourceID, ArrayList<String> sameAsList) {
    ArrayList<String> nonExistentVertices = new ArrayList<String>();
    ArrayList<String> existentVertices = new ArrayList<String>();
    if (isVertexExists(JanusGraphSchemaConstant.ID_PROPERTY, sourceID)) {
      existentVertices.add(sourceID);
    } else {
      nonExistentVertices.add(sourceID);
    }

    for (String vertexIRI : sameAsList) {
      if (isVertexExists(JanusGraphSchemaConstant.ID_PROPERTY, vertexIRI)) {
        existentVertices.add(vertexIRI);
      } else {
        nonExistentVertices.add(vertexIRI);
      }
    }

    String firstID;
    if (existentVertices.size() > 0) {
      firstID = existentVertices.remove(0);
    } else {
      firstID = nonExistentVertices.remove(0);
    }
    Vertex source = getVertex(JanusGraphSchemaConstant.ID_PROPERTY, firstID);
    for (String vertexIRI : nonExistentVertices) {
      g.V(source).property(JanusGraphSchemaConstant.ID_PROPERTY, vertexIRI)
          .property(JanusGraphSchemaConstant.ORG_ID_PROPERTY, vertexIRI).next();
    }

    for (String vertexIRI : existentVertices) {
      Vertex sameAsVertex = getVertex(JanusGraphSchemaConstant.ID_PROPERTY, vertexIRI);
      source = mergeVertex(source, sameAsVertex);
    }
    return (source);
  }

  /**
   * checks if vertex exists for propertyKey = propertyValue
   * 
   * @param propertyKey
   * @param propertyValue
   * @return
   */
  private boolean isVertexExists(String propertyKey, String propertyValue) {
    Optional<Vertex> optionalVertex = g.V().has(propertyKey, propertyValue).tryNext();
    if (optionalVertex.isPresent()) {
      return (true);
    } else {
      return (false);
    }
  }

  /**
   * This function merges two vertices. It merges smaller degree vertex to larger degree vertex
   * 
   * @param v1
   * @param v2
   * @return
   */
  private Vertex mergeVertex(Vertex v1, Vertex v2) {
    long v1EdgeCount = g.V(v1).both().count().next();
    long v2EdgeCount = g.V(v2).both().count().next();

    Vertex firstVertex;
    Vertex secondVertex;

    if (v1EdgeCount < v2EdgeCount) {
      firstVertex = v1;
      secondVertex = v2;
    } else {
      firstVertex = v2;
      secondVertex = v1;
    }

    for (String key : g.V(firstVertex).properties().key().toSet()) {
      Iterator<Object> i = g.V(firstVertex).values(key);
      while (i.hasNext()) {
        Object propVal = i.next();
        g.V(secondVertex).property(key, propVal).next();
      }

      GraphTraversal<Vertex, ? extends Property<Object>> metaProperties =
          g.V(firstVertex).properties(key).properties();
      while (metaProperties.hasNext()) {
        Property<Object> metaProp = metaProperties.next();
        String metaPropKey = metaProp.key();
        Object metaPropValue = metaProp.value();
        g.V(secondVertex).properties(key).property(metaPropKey, metaPropValue).next();
      }
    }
    GraphTraversal<Vertex, Edge> outEdges = g.V(firstVertex).outE();
    while (outEdges.hasNext()) {
      Edge edge = outEdges.next();
      Vertex v1_adj_Vertex = edge.inVertex();
      String edgeLabel = edge.label();
      g.V(secondVertex).as("f").V(v1_adj_Vertex).addE(edgeLabel).from("f").next();
    }

    GraphTraversal<Vertex, Edge> inEdges = g.V(firstVertex).inE();
    while (inEdges.hasNext()) {
      Edge edge = inEdges.next();
      Vertex vertex_adj_v1 = edge.outVertex();
      String edgeLabel = edge.label();
      g.V(vertex_adj_v1).as("f").V(secondVertex).addE(edgeLabel).from("f").next();
    }
    try {
      g.V(firstVertex).drop().iterate();
    } catch (Exception e) {
      e.printStackTrace();
    }
    return (secondVertex);
  }
}
