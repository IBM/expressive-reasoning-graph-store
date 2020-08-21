
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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.util.Properties;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
import org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV3d0;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry;


/**
 * This is main class for running ingestion modele. This is used for only for checking ingestion
 * module separately.
 * 
 * @author Sumit Neelam (sumit.neelam@in.ibm.com)
 *
 */
public class Main {
  public static void main(String args[]) {

    if (args.length < 1) {
      System.out.println("Insufficient arguments: property file missing");
      System.exit(0);
    }
    Properties properties = new Properties();
    try {
      properties.load(new FileReader(args[0]));
    } catch (Exception e) {
      System.out.println("Exception in input property File: " + e);
      System.exit(0);
    }

    long startTime = System.nanoTime();

    boolean SERVER_MODE = false;
    LoadRDFData loadRDFData;

    if (SERVER_MODE) {
      GryoMapper.Builder kryo = GryoMapper.build().addRegistry(JanusGraphIoRegistry.getInstance());
      MessageSerializer serializer = new GryoMessageSerializerV3d0(kryo);
      Cluster cluster = Cluster.build("localhost").serializer(serializer).create();
      Client client = cluster.connect("123456");

      String tboxFile = "src/test/resources/univ-bench.owl";
      InputStream i = null;
      try {
        i = new FileInputStream(tboxFile);
      } catch (Exception e) {
        e.printStackTrace();
        System.exit(0);
      }

      startTime = System.nanoTime();
      CreateGraphFoundationSchema schemaCreator =
          new CreateGraphFoundationSchema(client, "lubmtest", i, properties);
      schemaCreator.createGraphFoundationSchema();

      loadRDFData = new LoadRDFData(client, "lubmtest", properties);
      File loadDirectory = new File("src/main/resources/lubm1u-1File");
      RDFFormat rdfFormat = RDFFormat.RDFXML;
      String baseURI = "http://swat.cse.lehigh.edu/onto/univ-bench.owl";

      loadRDFData.loadDirectoryAllFiles(loadDirectory, rdfFormat, baseURI);

      client.close();
      cluster.close();
    } else {
      String tboxFile = "src/test/resources/univ-bench.owl";
      InputStream i = null;
      try {
        i = new FileInputStream(tboxFile);
      } catch (Exception e) {
        e.printStackTrace();
        System.exit(0);
      }
      JanusGraph janusGraph =
          JanusGraphFactory.open("src/test/resources/janusgraph-hbase.properties");

      startTime = System.nanoTime();
      CreateGraphFoundationSchema schemaCreator =
          new CreateGraphFoundationSchema(janusGraph, i, properties);
      schemaCreator.createGraphFoundationSchema();

      loadRDFData = new LoadRDFData(janusGraph, properties);

      File loadDirectory = new File("src/test/resources/lubm1u");
      RDFFormat rdfFormat = RDFFormat.RDFXML;
      String baseURI = "http://swat.cse.lehigh.edu/onto/univ-bench.owl";

      loadRDFData.loadDirectoryAllFiles(loadDirectory, rdfFormat, baseURI);
      // loadRDFData.loadFromFile(new File("src/main/resources/univ-bench.owl"), rdfFormat,
      // baseURI);
      janusGraph.close();
    }

    long totalExecTime = System.nanoTime() - startTime;
    System.out.println("\n\nLoading time: " + totalExecTime * 1.0 / 1E6);
    System.exit(0);
  }
}
