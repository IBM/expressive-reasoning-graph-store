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
package com.ibm.research.ergs.query.engine;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.Syntax;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.jsr223.JavaTranslator;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.QueryParserUtil;
import org.janusgraph.core.JanusGraph;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.ibm.research.ergs.query.translation.GenericQueryConversion;
import com.ibm.research.ergs.query.utils.CostModel;
import com.ibm.research.ergs.query.utils.PropertyMapping;
import com.ibm.research.owlql.ruleref.OWLQLSPARQLCompiler;

/**
 * It is used for executing SPARQL queries over JanusGraph
 *
 * @author Udit Sharma
 *
 */
public class SPARQLQueryProcessorJanusGraph implements SPARQLQueryProcessor {
  private static final Logger logger =
      LoggerFactory.getLogger(SPARQLQueryProcessorJanusGraph.class);

  private PropertyMapping propertyMapping;
  private GraphTraversalSource traversalSource;
  private CostModel costModel;
  private OWLQLSPARQLCompiler owlQlSparqlcompiler;

  /**
   * Constructs {@link SPARQLQueryProcessorJanusGraph} for local JanusGraphConnection
   *
   * @param graph JanusGraph instance
   * @param graphName name of graph/table in DB
   */
  public SPARQLQueryProcessorJanusGraph(JanusGraph graph, String graphName) {
    this.traversalSource = graph.traversal();
    this.propertyMapping = new PropertyMapping(this.traversalSource);
    this.costModel = new CostModel(graph, graphName);
    try {
      InputStream stream =
          new ByteArrayInputStream(propertyMapping.getTbox().getBytes(StandardCharsets.UTF_8));
      OWLOntology ont =
          OWLManager.createOWLOntologyManager().loadOntologyFromOntologyDocument(stream);
      this.owlQlSparqlcompiler = new OWLQLSPARQLCompiler(ont, null, null);
    } catch (OWLOntologyCreationException | NullPointerException e) {
      logger.warn("Tbox is either not available or invalid. Query expansion will not be used");
    }
  }

  /**
   * Constructs {@link SPARQLQueryProcessorJanusGraph} for remote janusgraph connection
   *
   * @param client Gremlin client
   * @param graphName name of graph/table in DB
   * @param traversalName name of graph traversal in DB
   */
  public SPARQLQueryProcessorJanusGraph(Client client, String graphName, String traversalName) {
    this.traversalSource = AnonymousTraversalSource.traversal()
        .withRemote(DriverRemoteConnection.using(client.getCluster(), traversalName));
    this.propertyMapping = new PropertyMapping(this.traversalSource);
    this.costModel = new CostModel(client, graphName, traversalName);
    try {
      InputStream stream =
          new ByteArrayInputStream(propertyMapping.getTbox().getBytes(StandardCharsets.UTF_8));
      OWLOntology ont =
          OWLManager.createOWLOntologyManager().loadOntologyFromOntologyDocument(stream);
      this.owlQlSparqlcompiler = new OWLQLSPARQLCompiler(ont, null, null);
    } catch (OWLOntologyCreationException | NullPointerException e) {
      logger.warn("Tbox is either not available or invalid. Query expansion will not be used");
    }
  }

  /**
   * It expands the SPARQL query using quetzal
   *
   * @param queryString original query
   * @return expand query if possible, original query otherwise
   */
  private String expandQuery(String queryString) {
    try {
      Query eq =
          owlQlSparqlcompiler.compile(QueryFactory.create(queryString, Syntax.syntaxSPARQL_11));
      return eq.toString();
    } catch (RuntimeException e) {
      logger.warn("Query expansion can't be used for the query");
      return queryString;
    }
  }

  @Override
  public QueryResultSet executeQuery(String queryString) {
    queryString = expandQuery(queryString);

    ParsedQuery query = QueryParserUtil.parseQuery(QueryLanguage.SPARQL, queryString, null);
    TupleExpr tupleExpr = query.getTupleExpr();

    logger.info("Query Parse Tree: " + tupleExpr);
    GenericQueryConversion queryConversion =
        new GenericQueryConversion(traversalSource, tupleExpr, propertyMapping, costModel);
    GraphTraversal<?, ?> traversal = queryConversion.getGremlinTraversal();

    logger.info("Gremlin Traversal: " + traversal);
    Bytecode traversalByteCode = traversal.asAdmin().getBytecode();
    Stream stream = JavaTranslator.of(traversalSource).translate(traversalByteCode).toStream();

    return new QueryResultSet(stream, queryConversion.getVariables());
  }

}
