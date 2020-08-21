
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.ReentrantLock;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.SimpleBNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.ibm.research.ergs.ingestion.graphdb.GraphDBLoad;
import com.ibm.research.ergs.ingestion.graphdb.JanusGraphConnection;
import com.ibm.research.ergs.ingestion.graphdb.SchemaCreator;

/**
 * This class reads batch of rdf data, partitions the batch and parallelly load in graph.
 * 
 * @author Sumit Neelam (sumit.neelam@in.ibm.com)
 *
 */
class TriplesBatch {

  private static final Logger logger = LoggerFactory.getLogger(TriplesBatch.class);

  boolean DEBUG = false;
  JanusGraphConnection janusGraphConnection;

  int triplesCount;
  double triplesLimit;
  int currentBuffer = 0;
  Set<String> vertexBuffer[];
  List<Statement> triplesBuffer[];
  List<Statement> confictTriplesBuffer[];
  Set<String> nodeSet;
  int commitFrequency;
  private ExecutorService pool;
  CountDownLatch latch;
  long graphOperationTime = 0;

  HashMap<String, String> allPropertyKeys;
  HashSet<String> allEdgeLabels;
  HashMap<String, String> allConflictingLabels;

  HashMap<String, String> cycleNewPropertyKeys;
  HashSet<String> cycleNewEdgeLabels;
  HashMap<String, String> cycleConflictingLabels;
  HashMap<String, String> cycleNewIndexProp;

  HashSet<String> cycleDeletePropertyKeys;
  HashSet<String> cycleDeleteEdgeLabels;

  ReentrantLock lock;
  int cycle = 0;
  int indexCount = 0;

  /**
   * Construct {@link TriplesBatch}, initializes all data variables
   * 
   * @param janusGraphConnection
   * @param pool
   */
  TriplesBatch(JanusGraphConnection janusGraphConnection, ExecutorService pool) {

    this.janusGraphConnection = janusGraphConnection;
    this.pool = pool;

    commitFrequency = InputParameters.COMMIT_FREQUENCY;
    triplesLimit = Math.ceil(commitFrequency / InputParameters.NUM_THREADS);
    nodeSet = new HashSet<String>();
    vertexBuffer = new Set[InputParameters.NUM_THREADS];
    triplesBuffer = new List[InputParameters.NUM_THREADS];
    confictTriplesBuffer = new List[InputParameters.NUM_THREADS];

    for (int i = 0; i < InputParameters.NUM_THREADS; i++) {
      vertexBuffer[i] = new HashSet<String>();
      triplesBuffer[i] = new ArrayList<Statement>();
      confictTriplesBuffer[i] = new ArrayList<Statement>();
    }

    allPropertyKeys = new HashMap<String, String>();
    allEdgeLabels = new HashSet<String>();
    allConflictingLabels = new HashMap<String, String>();

    cycleNewPropertyKeys = new HashMap<String, String>();
    cycleNewEdgeLabels = new HashSet<String>();
    cycleConflictingLabels = new HashMap<String, String>();
    cycleNewIndexProp = new HashMap<String, String>();

    cycleDeletePropertyKeys = new HashSet<String>();
    cycleDeleteEdgeLabels = new HashSet<String>();

    lock = new ReentrantLock();

    try {
      GraphDBLoad graphDBLoad = janusGraphConnection.getLoader();
      HashMap<String, String> propertyKeys = graphDBLoad.getPropertyKeys();
      Set<String> edgeLabels = graphDBLoad.getEdgeLabels();
      HashMap<String, String> conflictingLables = graphDBLoad.getConflictingLabels();
      int externalIndexCount = graphDBLoad.getExteralIndexCount();

      allPropertyKeys.putAll(propertyKeys);
      allEdgeLabels.addAll(edgeLabels);
      allConflictingLabels.putAll(conflictingLables);
      indexCount = externalIndexCount;

      graphDBLoad.commit();
    } catch (Exception e) {
      logger.error("Exception in closing graph traversal: ", e);
    }
  }

  /**
   * This function is called for each read triple and adds to one of the partition. It also
   * classifies triple to edge or property or conflicting triple (same predicate is used for edge
   * and property). For literal triples, their data type is stored for further use.
   * 
   * @param st: input triple
   */
  protected void addTripleToBuffer(Statement st) {
    /*
     * adding subject and object node ids in the set
     */
    String subject = st.getSubject().toString();
    String predicate = st.getPredicate().toString();
    String predicateLocalName = st.getPredicate().getLocalName();
    if (!ConstantParameters.FULL_PREDICATE_FLAG) {
      predicate = predicateLocalName;
    }
    int subjectBufferNum = -1;
    for (int i = 0; i < InputParameters.NUM_THREADS; i++) {
      if (vertexBuffer[i].contains(subject)) {
        subjectBufferNum = i;
      }
    }
    Value object = st.getObject();
    if (object instanceof IRI || object instanceof SimpleBNode) {
      String objectStr;
      if (object instanceof IRI) {
        objectStr = object.toString();
      } else {
        SimpleBNode bNode = (SimpleBNode) object;
        objectStr = bNode.getID();
      }

      int objectBufferNum = -1;
      for (int i = 0; i < InputParameters.NUM_THREADS; i++) {
        if (vertexBuffer[i].contains(objectStr)) {
          objectBufferNum = i;
        }
      }
      if (subjectBufferNum == objectBufferNum) {
        if (subjectBufferNum == -1) { // Subject and object do not belong to any buffer
          vertexBuffer[currentBuffer].add(subject);
          vertexBuffer[currentBuffer].add(objectStr);
          triplesBuffer[currentBuffer].add(st);
        } else {
          triplesBuffer[subjectBufferNum].add(st);
          if (nodeSet.contains(subject)) {
            nodeSet.remove(subject);
          }
          if (nodeSet.contains(objectStr)) {
            nodeSet.remove(objectStr);
          }
        }
      } else if (subjectBufferNum == -1) {
        vertexBuffer[currentBuffer].add(subject);
        if (objectBufferNum == currentBuffer) {
          triplesBuffer[currentBuffer].add(st);
          if (nodeSet.contains(objectStr)) {
            nodeSet.remove(objectStr);
          }
        } else {
          nodeSet.add(subject);
          confictTriplesBuffer[currentBuffer].add(st);
        }
      } else if (objectBufferNum == -1) {
        vertexBuffer[currentBuffer].add(objectStr);
        if (subjectBufferNum == currentBuffer) {
          triplesBuffer[currentBuffer].add(st);
          if (nodeSet.contains(subject)) {
            nodeSet.remove(subject);
          }
        } else {
          nodeSet.add(objectStr);
          if (predicateLocalName.equalsIgnoreCase("type")) {
            confictTriplesBuffer[subjectBufferNum].add(st);
          } else {
            confictTriplesBuffer[currentBuffer].add(st);
          }
        }
      } else {
        if (predicateLocalName.equalsIgnoreCase("type")) {
          confictTriplesBuffer[subjectBufferNum].add(st);
        } else {
          confictTriplesBuffer[currentBuffer].add(st);
        }
      }
      if (!allConflictingLabels.containsKey(predicate)) {
        if (allPropertyKeys.containsKey(predicate)) {
          String datatype = allPropertyKeys.get(predicate);
          allConflictingLabels.put(predicate, datatype);
          cycleConflictingLabels.put(predicate, datatype);

          allPropertyKeys.remove(predicate);
          cycleDeletePropertyKeys.add(predicate);
        } else {
          if (!allEdgeLabels.contains(predicate)) {
            allEdgeLabels.add(predicate);
            cycleNewEdgeLabels.add(predicate);
          }
        }
      }
    } else {
      Literal l = (Literal) object;
      String literalDatatype = l.getDatatype().getLocalName();
      if (literalDatatype.equalsIgnoreCase("langString")) {
        literalDatatype = "string";
      }
      if (subjectBufferNum != -1) {
        triplesBuffer[subjectBufferNum].add(st);
        if (nodeSet.contains(subject)) {
          nodeSet.remove(subject);
        }
      } else {
        vertexBuffer[currentBuffer].add(subject);
        triplesBuffer[currentBuffer].add(st);
      }
      if (!allConflictingLabels.containsKey(predicate)) {
        if (allEdgeLabels.contains(predicate)) {
          allConflictingLabels.put(predicate, literalDatatype);
          cycleConflictingLabels.put(predicate, literalDatatype);
          allEdgeLabels.remove(predicate);
          cycleDeleteEdgeLabels.add(predicate);
        } else if (!allPropertyKeys.containsKey(predicate)) {
          allPropertyKeys.put(predicate, literalDatatype);
          cycleNewPropertyKeys.put(predicate, literalDatatype);
        }
      }
    }
    triplesCount++;

    if (triplesCount >= triplesLimit) {
      triplesCount = 0;
      currentBuffer++;
      if (currentBuffer == InputParameters.NUM_THREADS) {
        writeTriplesBatch();
        currentBuffer = 0;
        logger.info("Loading Cycle:" + cycle);
        cycle++;
      }
    }
  }

  /**
   * This is called when one batch is read and partitioned into parts. This function first creates
   * graph schema for new properties and edges and then load intra-partition data followed by inter
   * partition
   */
  void writeTriplesBatch() {
    long startTime = System.nanoTime();
    writeSchemaData();

    latch = new CountDownLatch(InputParameters.NUM_THREADS);
    writeNonConflictingTriples();

    logger.debug("Waiting for non conflicing data insertion");
    try {
      latch.await();
    } catch (InterruptedException e) {
      logger.error("Exception in waiting for thread completion: " + e);
    }
    logger.debug("Non conflicing data insertion Done.");

    writeNodesInDatabase(nodeSet);

    latch = new CountDownLatch(InputParameters.NUM_THREADS);
    writeConflictingTriples();

    logger.debug("Waiting for conflicing data insertion");
    try {
      latch.await();
    } catch (InterruptedException e) {
      logger.error("Exception in waiting for thread completion: " + e);
    }
    logger.debug("Conflicing data insertion done");

    graphOperationTime += (System.nanoTime() - startTime);
  }

  /**
   * This function first creates graph schema for new properties and edges
   */
  void writeSchemaData() {
    SchemaCreator schemaCreator = janusGraphConnection.getSchemaCreator();
    for (String propertyKey : cycleNewPropertyKeys.keySet()) {
      String graphProp = propertyKey + "_prop";
      String datatype = cycleNewPropertyKeys.get(propertyKey);
      if (InputParameters.BUILD_GRAPH_INDEX_ALL_PROPS
          || InputParameters.GRAPH_INDEX.contains(propertyKey)) {
        if (ConstantParameters.MULTIVALUED_PROPERTY) {
          schemaCreator.createIndexProperty(graphProp, datatype, false, true, false);
        } else {
          schemaCreator.createIndexProperty(graphProp, datatype, false, false, false);
        }
        cycleNewIndexProp.put(propertyKey, "CompositeIndex");
      } else if (InputParameters.BUILD_EXTERNAL_INDEX_ALL_PROPS) {
        if (ConstantParameters.MULTIVALUED_PROPERTY) {
          schemaCreator.createExternalIndexProperty(graphProp,
              InputParameters.EXTERNAL_INDEX_PREFIX + indexCount, datatype, true, false,
              InputParameters.ALL_EXTERNAL_INDEXTYPE);
        } else {
          schemaCreator.createExternalIndexProperty(graphProp,
              InputParameters.EXTERNAL_INDEX_PREFIX + indexCount, datatype, false, false,
              InputParameters.ALL_EXTERNAL_INDEXTYPE);
        }
        cycleNewIndexProp.put(propertyKey, InputParameters.EXTERNAL_INDEX_PREFIX + indexCount);
        indexCount++;
      } else if (InputParameters.EXTERNAL_INDEX_STRING.contains(propertyKey)) {
        if (ConstantParameters.MULTIVALUED_PROPERTY) {
          schemaCreator.createExternalIndexProperty(graphProp,
              InputParameters.EXTERNAL_INDEX_PREFIX + indexCount, datatype, true, false, "string");
        } else {
          schemaCreator.createExternalIndexProperty(graphProp,
              InputParameters.EXTERNAL_INDEX_PREFIX + indexCount, datatype, false, false, "string");
        }
        cycleNewIndexProp.put(propertyKey, InputParameters.EXTERNAL_INDEX_PREFIX + indexCount);
        indexCount++;
      } else if (InputParameters.EXTERNAL_INDEX_TEXT.contains(propertyKey)) {
        if (ConstantParameters.MULTIVALUED_PROPERTY) {
          schemaCreator.createExternalIndexProperty(graphProp,
              InputParameters.EXTERNAL_INDEX_PREFIX + indexCount, datatype, true, false, "text");
        } else {
          schemaCreator.createExternalIndexProperty(graphProp,
              InputParameters.EXTERNAL_INDEX_PREFIX + indexCount, datatype, false, false, "text");
        }
        cycleNewIndexProp.put(propertyKey, InputParameters.EXTERNAL_INDEX_PREFIX + indexCount);
        indexCount++;
      } else if (InputParameters.EXTERNAL_INDEX_BOTH.contains(propertyKey)) {
        if (ConstantParameters.MULTIVALUED_PROPERTY) {
          schemaCreator.createExternalIndexProperty(graphProp,
              InputParameters.EXTERNAL_INDEX_PREFIX + indexCount, datatype, true, false,
              "textstring");
        } else {
          schemaCreator.createExternalIndexProperty(graphProp,
              InputParameters.EXTERNAL_INDEX_PREFIX + indexCount, datatype, false, false,
              "textstring");
        }
        cycleNewIndexProp.put(propertyKey, InputParameters.EXTERNAL_INDEX_PREFIX + indexCount);
        indexCount++;
      } else {
        if (ConstantParameters.MULTIVALUED_PROPERTY) {
          schemaCreator.createProperty(graphProp, datatype, true, false);
        } else {
          schemaCreator.createProperty(graphProp, datatype, false, false);
        }
      }
      schemaCreator.createProperty(propertyKey, "string", true, true);
    }
    for (String edgeLabel : cycleNewEdgeLabels) {
      schemaCreator.createEdgeLabel(edgeLabel + "_edge");
      schemaCreator.createProperty(edgeLabel, "string", true, true);
    }
    for (String elementKey : cycleConflictingLabels.keySet()) {
      if (cycleDeleteEdgeLabels.contains(elementKey)) {
        String graphProp = elementKey + "_prop";
        String datatype = cycleConflictingLabels.get(elementKey);
        if (InputParameters.BUILD_GRAPH_INDEX_ALL_PROPS
            || InputParameters.GRAPH_INDEX.contains(elementKey)) {
          if (ConstantParameters.MULTIVALUED_PROPERTY) {
            schemaCreator.createIndexProperty(graphProp, datatype, false, true, false);
          } else {
            schemaCreator.createIndexProperty(graphProp, datatype, false, false, false);
          }
          cycleNewIndexProp.put(elementKey, "CompositeIndex");
        } else if (InputParameters.BUILD_EXTERNAL_INDEX_ALL_PROPS) {
          if (ConstantParameters.MULTIVALUED_PROPERTY) {
            schemaCreator.createExternalIndexProperty(graphProp,
                InputParameters.EXTERNAL_INDEX_PREFIX + indexCount, datatype, true, false,
                InputParameters.ALL_EXTERNAL_INDEXTYPE);
          } else {
            schemaCreator.createExternalIndexProperty(graphProp,
                InputParameters.EXTERNAL_INDEX_PREFIX + indexCount, datatype, false, false,
                InputParameters.ALL_EXTERNAL_INDEXTYPE);
          }
          cycleNewIndexProp.put(elementKey, InputParameters.EXTERNAL_INDEX_PREFIX + indexCount);
          indexCount++;
        } else if (InputParameters.EXTERNAL_INDEX_STRING.contains(elementKey)) {
          if (ConstantParameters.MULTIVALUED_PROPERTY) {
            schemaCreator.createExternalIndexProperty(graphProp,
                InputParameters.EXTERNAL_INDEX_PREFIX + indexCount, datatype, true, false,
                "string");
          } else {
            schemaCreator.createExternalIndexProperty(graphProp,
                InputParameters.EXTERNAL_INDEX_PREFIX + indexCount, datatype, false, false,
                "string");
          }
          cycleNewIndexProp.put(elementKey, InputParameters.EXTERNAL_INDEX_PREFIX + indexCount);
          indexCount++;
        } else if (InputParameters.EXTERNAL_INDEX_TEXT.contains(elementKey)) {
          if (ConstantParameters.MULTIVALUED_PROPERTY) {
            schemaCreator.createExternalIndexProperty(graphProp,
                InputParameters.EXTERNAL_INDEX_PREFIX + indexCount, datatype, true, false, "text");
          } else {
            schemaCreator.createExternalIndexProperty(graphProp,
                InputParameters.EXTERNAL_INDEX_PREFIX + indexCount, datatype, false, false, "text");
          }
          cycleNewIndexProp.put(elementKey, InputParameters.EXTERNAL_INDEX_PREFIX + indexCount);
          indexCount++;
        } else if (InputParameters.EXTERNAL_INDEX_BOTH.contains(elementKey)) {
          if (ConstantParameters.MULTIVALUED_PROPERTY) {
            schemaCreator.createExternalIndexProperty(graphProp,
                InputParameters.EXTERNAL_INDEX_PREFIX + indexCount, datatype, true, false,
                "textstring");
          } else {
            schemaCreator.createExternalIndexProperty(graphProp,
                InputParameters.EXTERNAL_INDEX_PREFIX + indexCount, datatype, false, false,
                "textstring");
          }
          cycleNewIndexProp.put(elementKey, InputParameters.EXTERNAL_INDEX_PREFIX + indexCount);
          indexCount++;
        } else {
          if (ConstantParameters.MULTIVALUED_PROPERTY) {
            schemaCreator.createProperty(graphProp, datatype, true, false);
          } else {
            schemaCreator.createProperty(graphProp, datatype, false, false);
          }
        }
      } else if (cycleDeletePropertyKeys.contains(elementKey)) {
        schemaCreator.createEdgeLabel(elementKey + "_edge");
      } else {
        logger.error("ERROR: Interal error E1");
      }
    }
    schemaCreator.commit();

    try {
      GraphDBLoad graphDBLoad = janusGraphConnection.getLoader();
      graphDBLoad.writeSchemaInformation(cycleNewPropertyKeys, cycleNewEdgeLabels,
          cycleConflictingLabels, cycleNewIndexProp, cycleDeletePropertyKeys, cycleDeleteEdgeLabels,
          indexCount);
      graphDBLoad.commit();
    } catch (Exception e) {
      logger.error("Exception in closing graph traversal: ", e);
    }

    cycleNewPropertyKeys.clear();
    cycleNewEdgeLabels.clear();
    cycleConflictingLabels.clear();
    cycleNewIndexProp.clear();
    cycleDeletePropertyKeys.clear();
    cycleDeleteEdgeLabels.clear();
  }

  /**
   * This function writes intra-partition data.
   */
  void writeNonConflictingTriples() {
    for (int i = 0; i < InputParameters.NUM_THREADS; i++) {
      if (triplesBuffer[i].size() > 0) {
        writeTriplesInDatabase(triplesBuffer[i]);
      } else {
        latch.countDown();
      }
    }
  }

  /**
   * This function writes inter partition data.
   */
  void writeConflictingTriples() {
    for (int i = 0; i < InputParameters.NUM_THREADS; i++) {
      if (confictTriplesBuffer[i].size() > 0) {
        writeTriplesInDatabase(confictTriplesBuffer[i]);
      } else {
        latch.countDown();
      }
    }
  }

  /**
   * This function stores degree 0 vertex which does not belong to any partition.
   * 
   * @param nodeIds: set of degree 0 vertices
   */
  void writeNodesInDatabase(Set<String> nodeIds) {
    try {
      long startTime = System.nanoTime();
      GraphDBLoad graphDBLoad = janusGraphConnection.getLoader();

      graphDBLoad.createVertexSet(nodeIds);
      long startTime1 = System.nanoTime();
      graphDBLoad.commit();
      long endTime1 = System.nanoTime();
      long duration1 = (endTime1 - startTime1);
      logger.info("Commit time in thread: " + Long.toString(duration1 / 1000000) + " ms");
      long endTime = System.nanoTime();
      long duration = (endTime - startTime);
      logger.info("Execution done. Total Vertex:" + nodeIds.size());
      logger.info("Execution time: " + Long.toString(duration / 1000000) + " ms");
      nodeIds.clear();
    } catch (Exception e) {
      logger.error("Exception in closing graph traversal: ", e);
    }
  }

  /**
   * this is wrapper function for writing triples.
   * 
   * @param triples
   */
  void writeTriplesInDatabase(List<Statement> triples) {
    if (pool != null) {
      pool.submit(() -> {
        writeTriplesInDatabaseMain(triples);
      });
    } else {
      writeTriplesInDatabaseMain(triples);
    }
  }

  /**
   * This is main function for writing triples into graph. It is used for both intra and inter
   * partition data.
   * 
   * @param triples
   */
  void writeTriplesInDatabaseMain(List<Statement> triples) {
    try {
      String threadId = Thread.currentThread().getName();
      long startTime = System.nanoTime();
      GraphDBLoad graphDBLoad = janusGraphConnection.getLoader();

      String currentSource = null;
      HashMap<String, ArrayList<Literal>> properties = new HashMap<String, ArrayList<Literal>>();
      HashMap<String, ArrayList<String>> typeMap = new HashMap<String, ArrayList<String>>();
      HashMap<String, ArrayList<String>> edges = new HashMap<String, ArrayList<String>>();
      ArrayList<String> sameAsList = new ArrayList<String>();

      for (Statement st : triples) {
        Resource subject = st.getSubject();
        IRI predicate = st.getPredicate();
        Value object = st.getObject();

        if (currentSource == null || (!currentSource.equals(subject.toString()))) {
          if (currentSource != null) {
            graphDBLoad.createCompleteNode(currentSource, typeMap, properties, edges, sameAsList,
                allPropertyKeys, allConflictingLabels);
          }
          currentSource = subject.toString();
          properties.clear();
          edges.clear();
          typeMap.clear();
          sameAsList.clear();
        }
        String predicateString = predicate.toString();
        String predicateLocalName = predicate.getLocalName();
        if (!ConstantParameters.FULL_PREDICATE_FLAG) {
          predicateString = predicateLocalName;
        }

        if (object instanceof IRI) {
          IRI objectIRI = (IRI) object;
          if (predicateLocalName.equals("type")) {
            ArrayList<String> targets = typeMap.get(predicateString);
            if (targets == null) {
              targets = new ArrayList<String>();
              typeMap.put(predicateString, targets);
            }
            targets.add(objectIRI.toString());
          } else if (predicateLocalName.equals("sameAs")
              && predicateString.equals("http://www.w3.org/2002/07/owl#sameAs")) {
            sameAsList.add(objectIRI.toString());
          } else {
            ArrayList<String> targets = edges.get(predicateString);
            if (targets == null) {
              targets = new ArrayList<String>();
              edges.put(predicateString, targets);
            }
            targets.add(objectIRI.toString());
          }
        } else if (object instanceof SimpleBNode) {
          SimpleBNode bNode = (SimpleBNode) object;

          ArrayList<String> targets = edges.get(predicateString);
          if (targets == null) {
            targets = new ArrayList<String>();
            edges.put(predicateString, targets);
          }
          targets.add(bNode.toString());
        } else if (object instanceof Literal) {
          Literal l = (Literal) object;
          ArrayList<Literal> propList = properties.get(predicateString);
          if (propList == null) {
            propList = new ArrayList<Literal>();
            properties.put(predicateString, propList);
          }
          propList.add(l);
        }
      }
      graphDBLoad.createCompleteNode(currentSource, typeMap, properties, edges, sameAsList,
          allPropertyKeys, allConflictingLabels);
      long startTime1 = System.nanoTime();

      graphDBLoad.commit();

      long endTime1 = System.nanoTime();
      long duration1 = (endTime1 - startTime1);
      logger.info(
          "Commit time in thread " + threadId + ": " + Long.toString(duration1 / 1000000) + " ms");

      long endTime = System.nanoTime();
      long duration = (endTime - startTime);
      logger.info("Thread " + threadId + " execution done. Total Edges:" + triples.size());
      logger.info("Execution time: " + Long.toString(duration / 1000000) + " ms");
      triples.clear();
      latch.countDown();
    } catch (Exception e) {
      logger.error("Exception in graph operation: ", e);
    }
  }
}
