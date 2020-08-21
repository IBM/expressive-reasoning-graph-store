
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
import java.io.FileReader;
import java.util.Properties;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;

/**
 * Class contains bulk loader function executed from docker environment.
 * 
 * @author Sumit Neelam (sumit.neelam@in.ibm.com)
 *
 */
public class BulkLoader {
  public static void main(String args[]) {
    boolean success = true;
    if (args.length < 5) {
      System.out
          .println("Insufficient arguments. Following should be sructure of input arguments.");
      System.out
          .println("BulkLoader <graph name> <input file> <format> <base uri> <properties file>");
      success = false;
    }
    String graphName = null;
    File inputFile = null;
    RDFFormat rdfFormat = null;
    String baseURI = null;
    Properties properties = new Properties();
    try {
      graphName = args[0];
      inputFile = new File(args[1]);
      String format = args[2];
      baseURI = args[3];
      properties.load(new FileReader(args[4]));

      if (format.equalsIgnoreCase("rdf")) {
        rdfFormat = RDFFormat.RDFXML;
      } else if (format.equalsIgnoreCase("turtle")) {
        rdfFormat = RDFFormat.TURTLE;
      } else if (format.equalsIgnoreCase("ntriples")) {
        rdfFormat = RDFFormat.NTRIPLES;
      } else if (format.equalsIgnoreCase("n3")) {
        rdfFormat = RDFFormat.N3;
      } else {
        System.out.println("Unsupported input file type:" + format);
        System.out.println("Please choose one of following: 'rdf', 'turtle', 'ntriples', 'n3'");
        success = false;
      }
    } catch (Exception e) {
      System.out.println("Exception in input property File: " + e);
      success = false;
    }
    if (success) {
      Configuration configuration = new BaseConfiguration();
      configuration.addProperty("gremlin.graph", "org.janusgraph.core.JanusGraphFactory");
      configuration.addProperty("storage.backend", "cql");
      configuration.addProperty("storage.hostname", "cass-docker");
      configuration.addProperty("storage.cql.keyspace", graphName);
      configuration.addProperty("cache.db-cache", true);
      configuration.addProperty("cache.db-cache-clean-wait", 20);
      configuration.addProperty("cache.db-cache-time", 180000);
      configuration.addProperty("cache.db-cache-size", 0.5);
      configuration.addProperty("ids.block-size", 48000000);
      configuration.addProperty("storage.transactions", false);
      configuration.addProperty("storage.write-time", 1000000);
      configuration.addProperty("schema.default", "none");
      configuration.addProperty("storage.batch-loading", true);

      JanusGraph janusGraph = JanusGraphFactory.open(configuration);
      LoadRDFData loadRDFData = new LoadRDFData(janusGraph, properties);

      if (inputFile.isDirectory()) {
        loadRDFData.loadDirectoryAllFiles(inputFile, rdfFormat, baseURI);
      } else {
        loadRDFData.loadFromFile(inputFile, rdfFormat, baseURI);
      }

      janusGraph.close();
    }
  }
}
