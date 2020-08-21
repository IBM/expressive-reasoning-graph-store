
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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.Properties;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.ParserConfig;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.BasicParserSettings;
import org.janusgraph.core.JanusGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.ibm.research.ergs.ingestion.graphdb.JanusGraphConnection;
import com.ibm.research.ergs.ingestion.graphdb.JanusGraphConnectionDirectMode;
import com.ibm.research.ergs.ingestion.graphdb.JanusGraphConnectionServerMode;

/**
 * This is Entry class for loading RDF data into property graph.It performs following tasks:
 * 1.Creates appropriate janusgraph connection instance through constructor. 2.Creates RDF4J parser
 * object. 3.Load input RDF data based on type of input source.
 * 
 * @author Sumit Neelam (sumit.neelam@in.ibm.com)
 *
 */
public class LoadRDFData {

  private static final Logger logger = LoggerFactory.getLogger(LoadRDFData.class);

  JanusGraphConnection janusGraphConnection;

  /**
   * Constructs {@link LoadRDFData}
   * 
   * @param janusGraph: janusgraph object for direct loading
   * @param properties: loading related configuration
   * @param tboxStream: tbox ontology file stream
   */
  public LoadRDFData(JanusGraph janusGraph, Properties properties) {
    InputParameters.setParemeters(properties, getGraphName(janusGraph.configuration()));
    janusGraphConnection = new JanusGraphConnectionDirectMode(janusGraph);
  }

  /**
   * Constructs {@link LoadRDFData}
   * 
   * @param client: client object for server based loaading
   * @param graphName: name of graph in server
   * @param properties: loading related configuration
   * @param tboxStream: tbox ontology file stream
   */
  public LoadRDFData(Client client, String graphName, Properties properties) {
    InputParameters.setParemeters(properties, graphName);
    janusGraphConnection = new JanusGraphConnectionServerMode(client, graphName);
  }

  /**
   * Constructs {@link LoadRDFData}
   * 
   * @param janusGraph: janusgraph object for direct loading
   * @param properties: loading related configuration
   */
  // public LoadRDFData(JanusGraph janusGraph, Properties properties) {
  // this(janusGraph, properties, null);
  // }
  //
  // /**
  // *
  // * @param client: client object for server based loaading
  // * @param graphName: name of graph in server
  // * @param properties: loading related configuration
  // */
  // public LoadRDFData(Client client, String graphName, Properties properties) {
  // this(client, graphName, properties, null);
  // }

  /**
   * This function gets graphName from janusgraph Configuration, for external indexing purpose. This
   * is used in direct loading.
   * 
   * @param configuration: janusgraph configuration object
   * @return: graph Name
   */
  private String getGraphName(Configuration configuration) {
    if (configuration.containsKey("storage.hbase.table")) {
      return (configuration.getString("storage.hbase.table"));
    } else if (configuration.containsKey("storage.cql.keyspace")) {
      return (configuration.getString("storage.cql.keyspace"));
    } else if (configuration.containsKey("storage.cassandra.keyspace")) {
      return (configuration.getString("storage.cassandra.keyspace"));
    } else {
      logger.warn("Graph name not found from janusgraph object, using 'janusgraph' as default.");
      return ("janusgraph");
    }
  }

  /**
   * This function creates {@link RDFParser} object based on input format.
   * 
   * @param format: input data format
   * @param rdfReaderListener: custom rdfReader Listener class object.
   * @return
   */
  private RDFParser getRDFParser(RDFFormat format, RDFReaderListener rdfReaderListener) {
    RDFParser rdfParser = Rio.createParser(format);
    ParserConfig parserConfig = rdfParser.getParserConfig();
    parserConfig.set(BasicParserSettings.VERIFY_URI_SYNTAX, false);
    parserConfig.set(BasicParserSettings.VERIFY_DATATYPE_VALUES, false);
    parserConfig.set(BasicParserSettings.FAIL_ON_UNKNOWN_DATATYPES, false);
    rdfParser.setRDFHandler(rdfReaderListener);
    return (rdfParser);
  }

  /**
   * This function loads all files present in loadDirectory.
   * 
   * @param loadDirectory: rdf files directory
   * @param rdfFormat: rdf data format
   * @param baseURI: baseURI of data.
   */
  public void loadDirectoryAllFiles(File loadDirectory, RDFFormat rdfFormat, String baseURI) {
    RDFReaderListener rdfReaderListener = new RDFReaderListener(janusGraphConnection);
    RDFParser rdfParser = getRDFParser(rdfFormat, rdfReaderListener);
    if (loadDirectory.isDirectory()) {
      File[] listOfFiles = loadDirectory.listFiles();
      for (int i = 0; i < listOfFiles.length; i++) {
        File file = listOfFiles[i];
        loadSingleFile(file, rdfParser, baseURI);
      }
      rdfReaderListener.finalCommit();
    } else {
      logger.error("No directory found: " + loadDirectory);
    }
  }

  /**
   * This function is used by loadDirectoryAllFiles() function for individual file loading.
   * 
   * @param inputFile
   * @param rdfParser
   * @param baseURI
   */
  private void loadSingleFile(File inputFile, RDFParser rdfParser, String baseURI) {
    try {
      InputStream in = new FileInputStream(inputFile);
      rdfParser.parse(in, baseURI);
      in.close();
    } catch (FileNotFoundException e) {
      logger.error("Exception: input file not found: " + inputFile, e);
      logger.error("Skipping file from loading");
    } catch (IOException e) {
      logger.error("Exception in processing input rdf data file: ", e);
    }
  }

  /**
   * This function loads single file.
   * 
   * @param inputFile: input file
   * @param rdfFormat: rdf data format
   * @param baseURI: baseURI of data.
   */
  public void loadFromFile(File inputFile, RDFFormat rdfFormat, String baseURI) {
    try {
      RDFReaderListener rdfReaderListener = new RDFReaderListener(janusGraphConnection);
      RDFParser rdfParser = getRDFParser(rdfFormat, rdfReaderListener);

      InputStream in = new FileInputStream(inputFile);
      rdfParser.parse(in, baseURI);
      in.close();
      rdfReaderListener.finalCommit();

    } catch (FileNotFoundException e) {
      logger.error("Exception: input file not found: " + inputFile, e);
      logger.error("Skipping file from loading");
    } catch (IOException e) {
      logger.error("Exception in processing input rdf data file: " + e);
    }
  }

  /**
   * loads data from from input stream
   * 
   * @param in: input stream
   * @param format: rdf data format
   * @param baseURI: baseURI of data.
   */
  public void loadFromInputStream(InputStream in, RDFFormat format, String baseURI) {
    try {
      RDFReaderListener rdfReaderListener = new RDFReaderListener(janusGraphConnection);
      RDFParser rdfParser = getRDFParser(format, rdfReaderListener);
      rdfParser.parse(in, baseURI);
      rdfReaderListener.finalCommit();
    } catch (IOException e) {
      logger.error("Exception in processing input rdf data: ", e);
    }
  }

  /**
   * loads data from reader object
   * 
   * @param reader
   * @param format: rdf data format
   * @param baseURI: baseURI of data.
   */
  public void loadFromReader(Reader reader, RDFFormat format, String baseURI) {
    try {
      RDFReaderListener rdfReaderListener = new RDFReaderListener(janusGraphConnection);
      RDFParser rdfParser = getRDFParser(format, rdfReaderListener);
      rdfParser.parse(reader, baseURI);
      rdfReaderListener.finalCommit();
    } catch (Exception e) {
      logger.error("Exception in processing input rdf data: ", e);
    }
  }

  /**
   * loads individual rdf statement
   * 
   * @param stmt
   */
  public void loadRDFStatement(Statement stmt) {
    RDFReaderListener rdfReaderListener = new RDFReaderListener(janusGraphConnection);
    rdfReaderListener.handleStatement(stmt);
    rdfReaderListener.finalCommit();
  }
}
