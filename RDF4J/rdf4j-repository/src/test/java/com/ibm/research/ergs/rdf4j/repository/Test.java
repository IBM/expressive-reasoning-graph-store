/*******************************************************************************
 * Copyright 2020 IBM Corporation and others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package com.ibm.research.ergs.rdf4j.repository;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.GraphQuery;
import org.eclipse.rdf4j.query.GraphQueryResult;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.bigtable.repackaged.com.google.common.collect.ImmutableMap;
import com.ibm.research.ergs.query.utils.RDF4JHelper;
import com.ibm.research.ergs.rdf4j.config.SystemConfig;
import junit.framework.TestCase;

public class Test extends TestCase {
  private static final Logger logger = LoggerFactory.getLogger(Test.class);
  private Repository rep = null;

  @Override
  protected void setUp() throws Exception {
    logger.info("Creating ERGS inmemory repository");
    SystemConfig.setStorageBackend("inmemory");
    SystemConfig.setStorageHostName("localhost");
    SystemConfig.setIndexDirectory("index");
    rep = new ExpressiveReasoningGraphStoreRepository("test", false);
    rep.init();
  }

  @Override
  protected void tearDown() throws Exception {
    rep.shutDown();
    logger.info("Deleting ERGS repository");
    // JanusGraphBase.deleteGraph("test");
  }

  private void ingestData(String data) throws RDFParseException, RepositoryException, IOException {
    RepositoryConnection conn = rep.getConnection();
    InputStream stream = new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8));
    conn.add(stream, "", RDFFormat.TURTLE);
    conn.commit();
    conn.close();
  }

  private List<Map<String, String>> executeSelectQuery(String query, List<String> resultVariables) {
    List<Map<String, String>> actualOutput = new ArrayList<Map<String, String>>();
    DecimalFormat df = new DecimalFormat("0.00");
    try (RepositoryConnection conn = rep.getConnection()) {
      TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);

      try (TupleQueryResult result = tupleQuery.evaluate()) {
        while (result.hasNext()) { // iterate over the result
          BindingSet bindingSet = result.next();
          Map<String, String> row = new HashMap<String, String>();
          for (String variable : bindingSet.getBindingNames()) {
            if (resultVariables.contains(variable)) {
              Object obj = RDF4JHelper.getValue(bindingSet.getValue(variable));
              if (obj instanceof Float | obj instanceof Double) {
                row.put(variable, df.format(obj));
              } else {
                row.put(variable, obj.toString());
              }
            }
          }
          actualOutput.add(row);
        }
      }
      conn.close();
    }
    return actualOutput;
  }

  private List<Map<String, String>> executeGraphQuery(String query, List<String> resultVariables) {
    List<Map<String, String>> actualOutput = new ArrayList<Map<String, String>>();
    try (RepositoryConnection conn = rep.getConnection()) {
      GraphQuery graphQuery = conn.prepareGraphQuery(QueryLanguage.SPARQL, query);
      try (GraphQueryResult result = graphQuery.evaluate()) {
        while (result.hasNext()) { // iterate over the result
          Statement st = result.next();
          Map<String, String> row = new HashMap<String, String>();
          if (resultVariables.contains("subject")) {
            row.put("subject", st.getSubject().stringValue());
          }
          if (resultVariables.contains("predicate")) {
            row.put("predicate", st.getPredicate().stringValue());
          }
          if (resultVariables.contains("object")) {
            row.put("object", st.getObject().stringValue());
          }
          actualOutput.add(row);
        }
      }
    }
    return actualOutput;
  }

  public String getMessage(String data, String query, Collection<Map<String, String>> actualOutput,
      Collection<Map<String, String>> expectedOutput) {
    StringBuilder message = new StringBuilder();
    message.append("\n=======================\n");
    message.append("Data: " + data + "\n");
    message.append("query: " + query + "\n");
    message.append("Actual Output: " + actualOutput + "\n");
    message.append("ExpectedOutput: " + expectedOutput + "\n");
    message.append("=======================\n");
    return message.toString();
  }

  /**
   * Constructs Tested: SELECT, WHERE
   */
  public void test2_1() throws RDFParseException, RepositoryException, IOException {
    String data =
        "<http://example.org/book/book1> <http://purl.org/dc/elements/1.1/title> \"SPARQL Tutorial\" .";
    String query = "SELECT ?title\n" + "WHERE\n" + "{\n"
        + "  <http://example.org/book/book1> <http://purl.org/dc/elements/1.1/title> ?title .\n"
        + "}";
    Set<Map<String, String>> expectedOutput = new HashSet<Map<String, String>>();
    expectedOutput.add(ImmutableMap.of("title", "SPARQL Tutorial"));

    ingestData(data);
    Set<Map<String, String>> actualOutput =
        new HashSet<Map<String, String>>(executeSelectQuery(query, Arrays.asList("title")));

    logger.info(getMessage(data, query, actualOutput, expectedOutput));
    assertEquals(expectedOutput, actualOutput);
  }

  /**
   * Constructs Tested: SELECT, WHERE
   */
  public void test2_2() throws RDFParseException, RepositoryException, IOException {
    String data = "@prefix foaf:  <http://xmlns.com/foaf/0.1/> .\n" + "\n"
        + "_:a  foaf:name   \"Johnny Lee Outlaw\" .\n"
        + "_:a  foaf:mbox   <mailto:jlow@example.com> .\n"
        + "_:b  foaf:name   \"Peter Goodguy\" .\n"
        + "_:b  foaf:mbox   <mailto:peter@example.org> .\n"
        + "_:c  foaf:mbox   <mailto:carol@example.org> .";
    String query = "PREFIX foaf:   <http://xmlns.com/foaf/0.1/>\n" + "SELECT ?name ?mbox\n"
        + "WHERE\n" + "  { ?x foaf:name ?name .\n" + "    ?x foaf:mbox ?mbox }";
    Set<Map<String, String>> expectedOutput = new HashSet<Map<String, String>>();
    expectedOutput
        .add(ImmutableMap.of("name", "Johnny Lee Outlaw", "mbox", "mailto:jlow@example.com"));
    expectedOutput
        .add(ImmutableMap.of("name", "Peter Goodguy", "mbox", "mailto:peter@example.org"));

    ingestData(data);
    Set<Map<String, String>> actualOutput =
        new HashSet<Map<String, String>>(executeSelectQuery(query, Arrays.asList("name", "mbox")));

    logger.info(getMessage(data, query, actualOutput, expectedOutput));
    assertEquals(expectedOutput, actualOutput);
  }

  /**
   * Constructs Tested: SELECT, WHERE, BNodes
   */
  public void test2_4() throws RDFParseException, RepositoryException, IOException {
    String data = "@prefix foaf:  <http://xmlns.com/foaf/0.1/> .\n"
        + "_:a  foaf:name   \"Alice\" .\n" + "_:b  foaf:name   \"Bob\" .";
    String query = "PREFIX foaf:   <http://xmlns.com/foaf/0.1/>\n" + "SELECT ?x ?name\n"
        + "WHERE  { ?x foaf:name ?name }";
    Set<Map<String, String>> expectedOutput = new HashSet<Map<String, String>>();
    expectedOutput.add(ImmutableMap.of("name", "Alice"));
    expectedOutput.add(ImmutableMap.of("name", "Bob"));

    ingestData(data);
    List<Map<String, String>> actualOutput = executeSelectQuery(query, Arrays.asList("name"));

    logger.info(getMessage(data, query, actualOutput, expectedOutput));
    assertEquals(expectedOutput, new HashSet<Map<String, String>>(actualOutput));

  }

  /**
   * Constructs Tested: CONSTRUCT, WHERE
   */
  public void test2_6() throws RDFParseException, RepositoryException, IOException {
    String data = "@prefix org:    <http://example.com/ns#> .\n" + "\n"
        + "_:a  org:employeeName   \"Alice\" .\n" + "_:a  org:employeeId     12345 .\n" + "\n"
        + "_:b  org:employeeName   \"Bob\" .\n" + "_:b  org:employeeId     67890 .";
    String query = "PREFIX foaf:   <http://xmlns.com/foaf/0.1/>\n"
        + "PREFIX org:    <http://example.com/ns#>\n" + "\n" + "CONSTRUCT { ?x foaf:name ?name }\n"
        + "WHERE  { ?x org:employeeName ?name }";
    Set<Map<String, String>> expectedOutput = new HashSet<Map<String, String>>();
    expectedOutput
        .add(ImmutableMap.of("predicate", "http://xmlns.com/foaf/0.1/name", "object", "Alice"));
    expectedOutput
        .add(ImmutableMap.of("predicate", "http://xmlns.com/foaf/0.1/name", "object", "Bob"));

    ingestData(data);
    List<Map<String, String>> actualOutput =
        executeGraphQuery(query, Arrays.asList("predicate", "object"));

    logger.info(getMessage(data, query, actualOutput, expectedOutput));
    assertEquals(expectedOutput, new HashSet<Map<String, String>>(actualOutput));

  }

  /**
   * Constructs Tested: SELECT, WHERE, FILTER Regex
   */
  public void test3_1() throws RDFParseException, RepositoryException, IOException {
    String data = "@prefix dc:   <http://purl.org/dc/elements/1.1/> .\n"
        + "@prefix :     <http://example.org/book/> .\n"
        + "@prefix ns:   <http://example.org/ns#> .\n" + "\n"
        + ":book1  dc:title  \"SPARQL Tutorial\" .\n" + ":book1  ns:price  42 .\n"
        + ":book2  dc:title  \"The Semantic Web\" .\n" + ":book2  ns:price  23 .";
    String query = "PREFIX  dc:  <http://purl.org/dc/elements/1.1/>\n" + "SELECT  ?title\n"
        + "WHERE   { ?x dc:title ?title\n" + "          FILTER regex(?title, \"web\", \"i\" ) \n"
        + "        }";
    Set<Map<String, String>> expectedOutput = new HashSet<Map<String, String>>();
    expectedOutput.add(ImmutableMap.of("title", "The Semantic Web"));

    ingestData(data);
    List<Map<String, String>> actualOutput = executeSelectQuery(query, Arrays.asList("title"));

    logger.info(getMessage(data, query, actualOutput, expectedOutput));
    assertEquals(expectedOutput, new HashSet<Map<String, String>>(actualOutput));

  }

  /**
   * Constructs Tested: SELECT, WHERE, FILTER
   */
  public void test3_2() throws RDFParseException, RepositoryException, IOException {
    String data = "@prefix dc:   <http://purl.org/dc/elements/1.1/> .\n"
        + "@prefix :     <http://example.org/book/> .\n"
        + "@prefix ns:   <http://example.org/ns#> .\n" + "\n"
        + ":book1  dc:title  \"SPARQL Tutorial\" .\n" + ":book1  ns:price  42 .\n"
        + ":book2  dc:title  \"The Semantic Web\" .\n" + ":book2  ns:price  23 .";
    String query = "PREFIX  dc:  <http://purl.org/dc/elements/1.1/>\n"
        + "PREFIX  ns:  <http://example.org/ns#>\n" + "SELECT  ?title ?price\n"
        + "WHERE   { ?x ns:price ?price .\n" + "          FILTER (?price<30.5)\n"
        + "          ?x dc:title ?title . }";
    Set<Map<String, String>> expectedOutput = new HashSet<Map<String, String>>();
    expectedOutput.add(ImmutableMap.of("title", "The Semantic Web", "price", "23"));
    ingestData(data);
    Set<Map<String, String>> actualOutput = new HashSet<Map<String, String>>(
        executeSelectQuery(query, Arrays.asList("title", "price")));

    logger.info(getMessage(data, query, actualOutput, expectedOutput));
    assertEquals(expectedOutput, actualOutput);

  }

  /**
   * Constructs Tested: SELECT, WHERE, OPTIONAL
   */
  public void test6_1() throws RDFParseException, RepositoryException, IOException {
    String data = "@prefix foaf:       <http://xmlns.com/foaf/0.1/> .\n"
        + "@prefix rdf:        <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n" + "\n"
        + "_:a  rdf:type        foaf:Person .\n" + "_:a  foaf:name       \"Alice\" .\n"
        + "_:a  foaf:mbox       <mailto:alice@example.com> .\n"
        + "_:a  foaf:mbox       <mailto:alice@work.example> .\n" + "\n"
        + "_:b  rdf:type        foaf:Person .\n" + "_:b  foaf:name       \"Bob\" .";
    String query = "PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n" + "SELECT ?name ?mbox\n"
        + "WHERE  { ?x foaf:name  ?name .\n" + "         OPTIONAL { ?x  foaf:mbox  ?mbox }\n"
        + "       }";
    Set<Map<String, String>> expectedOutput = new HashSet<Map<String, String>>();
    expectedOutput.add(ImmutableMap.of("name", "Alice", "mbox", "mailto:alice@example.com"));
    expectedOutput.add(ImmutableMap.of("name", "Alice", "mbox", "mailto:alice@work.example"));
    expectedOutput.add(ImmutableMap.of("name", "Bob"));

    ingestData(data);
    Set<Map<String, String>> actualOutput =
        new HashSet<Map<String, String>>(executeSelectQuery(query, Arrays.asList("name", "mbox")));

    logger.info(getMessage(data, query, actualOutput, expectedOutput));
    assertEquals(expectedOutput, actualOutput);

  }

  /**
   * Constructs Tested: SELECT, WHERE, OPTIONAL with FILTER
   */
  public void test6_2() throws RDFParseException, RepositoryException, IOException {
    String data = "@prefix dc:   <http://purl.org/dc/elements/1.1/> .\n"
        + "@prefix :     <http://example.org/book/> .\n"
        + "@prefix ns:   <http://example.org/ns#> .\n" + "\n"
        + ":book1  dc:title  \"SPARQL Tutorial\" .\n" + ":book1  ns:price  42 .\n"
        + ":book2  dc:title  \"The Semantic Web\" .\n" + ":book2  ns:price  23 .";
    String query = "PREFIX  dc:  <http://purl.org/dc/elements/1.1/>\n"
        + "PREFIX  ns:  <http://example.org/ns#>\n" + "SELECT  ?title ?price\n"
        + "WHERE   { ?x dc:title ?title .\n"
        + "          OPTIONAL { ?x ns:price ?price . FILTER (?price < 30) }\n" + "        }";
    Set<Map<String, String>> expectedOutput = new HashSet<Map<String, String>>();
    expectedOutput.add(ImmutableMap.of("title", "SPARQL Tutorial"));
    expectedOutput.add(ImmutableMap.of("title", "The Semantic Web", "price", "23"));

    ingestData(data);
    Set<Map<String, String>> actualOutput = new HashSet<Map<String, String>>(
        executeSelectQuery(query, Arrays.asList("title", "price")));

    logger.info(getMessage(data, query, actualOutput, expectedOutput));
    assertEquals(expectedOutput, actualOutput);

  }

  /**
   * Constructs Tested: SELECT, WHERE, multiple OPTIONALs
   */
  public void test6_3() throws RDFParseException, RepositoryException, IOException {
    String data = "@prefix foaf:       <http://xmlns.com/foaf/0.1/> .\n" + "\n"
        + "_:a  foaf:name       \"Alice\" .\n"
        + "_:a  foaf:homepage   <http://work.example.org/alice/> .\n" + "\n"
        + "_:b  foaf:name       \"Bob\" .\n" + "_:b  foaf:mbox       <mailto:bob@work.example> .";
    String query = "PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n" + "SELECT ?name ?mbox ?hpage\n"
        + "WHERE  { ?x foaf:name  ?name .\n" + "         OPTIONAL { ?x foaf:mbox ?mbox } .\n"
        + "         OPTIONAL { ?x foaf:homepage ?hpage }\n" + "       }";
    Set<Map<String, String>> expectedOutput = new HashSet<Map<String, String>>();
    expectedOutput.add(ImmutableMap.of("name", "Alice", "hpage", "http://work.example.org/alice/"));
    expectedOutput.add(ImmutableMap.of("name", "Bob", "mbox", "mailto:bob@work.example"));

    ingestData(data);
    Set<Map<String, String>> actualOutput = new HashSet<Map<String, String>>(
        executeSelectQuery(query, Arrays.asList("name", "mbox", "hpage")));

    logger.info(getMessage(data, query, actualOutput, expectedOutput));
    assertEquals(expectedOutput, actualOutput);

  }

  /**
   * Constructs Tested: SELECT, WHERE, UNION
   */
  public void test7_a() throws RDFParseException, RepositoryException, IOException {
    String data = "@prefix dc10:  <http://purl.org/dc/elements/1.0/> .\n"
        + "@prefix dc11:  <http://purl.org/dc/elements/1.1/> .\n" + "\n"
        + "_:a  dc10:title     \"SPARQL Query Language Tutorial\" .\n"
        + "_:a  dc10:creator   \"Alice\" .\n" + "\n"
        + "_:b  dc11:title     \"SPARQL Protocol Tutorial\" .\n" + "_:b  dc11:creator   \"Bob\" .\n"
        + "\n" + "_:c  dc10:title     \"SPARQL\" .\n"
        + "_:c  dc11:title     \"SPARQL (updated)\" .";
    String query = "PREFIX dc10:  <http://purl.org/dc/elements/1.0/>\n"
        + "PREFIX dc11:  <http://purl.org/dc/elements/1.1/>\n" + "\n" + "SELECT ?title\n"
        + "WHERE  { { ?book dc10:title  ?title } UNION { ?book dc11:title  ?title } }";
    Set<Map<String, String>> expectedOutput = new HashSet<Map<String, String>>();
    expectedOutput.add(ImmutableMap.of("title", "SPARQL Protocol Tutorial"));
    expectedOutput.add(ImmutableMap.of("title", "SPARQL"));
    expectedOutput.add(ImmutableMap.of("title", "SPARQL (updated)"));
    expectedOutput.add(ImmutableMap.of("title", "SPARQL Query Language Tutorial"));

    ingestData(data);
    Set<Map<String, String>> actualOutput =
        new HashSet<Map<String, String>>(executeSelectQuery(query, Arrays.asList("title")));

    logger.info(getMessage(data, query, actualOutput, expectedOutput));
    assertEquals(expectedOutput, actualOutput);

  }

  /**
   * Constructs Tested: SELECT, WHERE, UNION
   */
  public void test7_b() throws RDFParseException, RepositoryException, IOException {
    String data = "@prefix dc10:  <http://purl.org/dc/elements/1.0/> .\n"
        + "@prefix dc11:  <http://purl.org/dc/elements/1.1/> .\n" + "\n"
        + "_:a  dc10:title     \"SPARQL Query Language Tutorial\" .\n"
        + "_:a  dc10:creator   \"Alice\" .\n" + "\n"
        + "_:b  dc11:title     \"SPARQL Protocol Tutorial\" .\n" + "_:b  dc11:creator   \"Bob\" .\n"
        + "\n" + "_:c  dc10:title     \"SPARQL\" .\n"
        + "_:c  dc11:title     \"SPARQL (updated)\" .";
    String query = "PREFIX dc10:  <http://purl.org/dc/elements/1.0/>\n"
        + "PREFIX dc11:  <http://purl.org/dc/elements/1.1/>\n" + "\n" + "SELECT ?x ?y\n"
        + "WHERE  { { ?book dc10:title ?x } UNION { ?book dc11:title  ?y } }";
    Set<Map<String, String>> expectedOutput = new HashSet<Map<String, String>>();
    expectedOutput.add(ImmutableMap.of("y", "SPARQL (updated)"));
    expectedOutput.add(ImmutableMap.of("y", "SPARQL Protocol Tutorial"));
    expectedOutput.add(ImmutableMap.of("x", "SPARQL"));
    expectedOutput.add(ImmutableMap.of("x", "SPARQL Query Language Tutorial"));

    ingestData(data);
    Set<Map<String, String>> actualOutput =
        new HashSet<Map<String, String>>(executeSelectQuery(query, Arrays.asList("x", "y")));

    logger.info(getMessage(data, query, actualOutput, expectedOutput));
    assertEquals(expectedOutput, actualOutput);

  }

  /**
   * Constructs tested: SELECT, WHERE, FILTER NOT EXISTS
   */
  public void test8_1_1() throws RDFParseException, RepositoryException, IOException {
    String data = "@prefix  :       <http://example/> .\n"
        + "@prefix  rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n"
        + "@prefix  foaf:   <http://xmlns.com/foaf/0.1/> .\n" + "\n"
        + ":alice  rdf:type   foaf:Person .\n" + ":alice  foaf:name  \"Alice\" .\n"
        + ":bob    rdf:type   foaf:Person .     ";
    String query = "PREFIX  rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n"
        + "PREFIX  foaf:   <http://xmlns.com/foaf/0.1/> \n" + "\n" + "SELECT ?person\n" + "WHERE \n"
        + "{\n" + "    ?person rdf:type  foaf:Person .\n"
        + "    FILTER NOT EXISTS { ?person foaf:name ?name }\n" + "}  ";
    Set<Map<String, String>> expectedOutput = new HashSet<Map<String, String>>();
    expectedOutput.add(ImmutableMap.of("person", "http://example/bob"));

    ingestData(data);
    Set<Map<String, String>> actualOutput =
        new HashSet<Map<String, String>>(executeSelectQuery(query, Arrays.asList("person")));

    logger.info(getMessage(data, query, actualOutput, expectedOutput));
    assertEquals(expectedOutput, actualOutput);
  }

  /**
   * Constructs tested: SELECT, WHERE, FILTER EXISTS
   */
  public void test8_1_2() throws RDFParseException, RepositoryException, IOException {
    String data = "@prefix  :       <http://example/> .\n"
        + "@prefix  rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n"
        + "@prefix  foaf:   <http://xmlns.com/foaf/0.1/> .\n" + "\n"
        + ":alice  rdf:type   foaf:Person .\n" + ":alice  foaf:name  \"Alice\" .\n"
        + ":bob    rdf:type   foaf:Person .     ";
    String query = "PREFIX  rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n"
        + "PREFIX  foaf:   <http://xmlns.com/foaf/0.1/> \n" + "\n" + "SELECT ?person\n" + "WHERE \n"
        + "{\n" + "    ?person rdf:type  foaf:Person .\n"
        + "    FILTER EXISTS { ?person foaf:name ?name }\n" + "}";
    Set<Map<String, String>> expectedOutput = new HashSet<Map<String, String>>();
    expectedOutput.add(ImmutableMap.of("person", "http://example/alice"));

    ingestData(data);
    Set<Map<String, String>> actualOutput =
        new HashSet<Map<String, String>>(executeSelectQuery(query, Arrays.asList("person")));

    logger.info(getMessage(data, query, actualOutput, expectedOutput));
    assertEquals(expectedOutput, actualOutput);

  }

  /**
   * Constructs tested: SELECT, WHERE, MINUS
   */
  public void test8_2() throws RDFParseException, RepositoryException, IOException {
    String data = "@prefix :       <http://example/> .\n"
        + "@prefix foaf:   <http://xmlns.com/foaf/0.1/> .\n" + "\n"
        + ":alice  foaf:givenName \"Alice\" ;\n" + "        foaf:familyName \"Smith\" .\n" + "\n"
        + ":bob    foaf:givenName \"Bob\" ;\n" + "        foaf:familyName \"Jones\" .\n" + "\n"
        + ":carol  foaf:givenName \"Carol\" ;\n" + "        foaf:familyName \"Smith\" .";
    String query =
        "PREFIX :       <http://example/>\n" + "PREFIX foaf:   <http://xmlns.com/foaf/0.1/>\n"
            + "\n" + "SELECT DISTINCT ?s\n" + "WHERE {\n" + "   ?s ?p ?o .\n" + "   MINUS {\n"
            + "      ?s foaf:givenName \"Bob\" .\n" + "   }\n" + "}";
    Set<Map<String, String>> expectedOutput = new HashSet<Map<String, String>>();
    expectedOutput.add(ImmutableMap.of("s", "http://example/alice"));
    expectedOutput.add(ImmutableMap.of("s", "http://example/carol"));

    ingestData(data);
    Set<Map<String, String>> actualOutput =
        new HashSet<Map<String, String>>(executeSelectQuery(query, Arrays.asList("s")));

    logger.info(getMessage(data, query, actualOutput, expectedOutput));
    assertEquals(expectedOutput, actualOutput);

  }

  /**
   * Constructs tested: SELECT, WHERE, FILTER NOT EXISTS/MINUS(with Disjoint variables)
   */
  public void test8_3_1() throws RDFParseException, RepositoryException, IOException {
    String data = "@prefix : <http://example/> .\n" + ":a :b :c .";
    ingestData(data);
    String queryFilterNotExists =
        "SELECT ?s ?p ?o" + "{ \n" + "  ?s ?p ?o\n" + "  FILTER NOT EXISTS { ?x ?y ?z }\n" + "}";
    Set<Map<String, String>> expectedOutputFilterNotExists = new HashSet<Map<String, String>>();
    Set<Map<String, String>> actualOutputFilterNotExists = new HashSet<Map<String, String>>(
        executeSelectQuery(queryFilterNotExists, Arrays.asList("s", "p", "o")));

    logger.info(getMessage(data, queryFilterNotExists, actualOutputFilterNotExists,
        expectedOutputFilterNotExists));
    assertEquals(expectedOutputFilterNotExists, actualOutputFilterNotExists);

    String queryMinus =
        "SELECT ?s ?p ?o" + "{ \n" + "  ?s ?p ?o\n" + "  MINUS { ?x ?y ?z }\n" + "}";
    Set<Map<String, String>> expectedOutputMinus = new HashSet<Map<String, String>>();
    expectedOutputMinus.add(
        ImmutableMap.of("s", "http://example/a", "p", "http://example/b", "o", "http://example/c"));
    Set<Map<String, String>> actualOutputMinus = new HashSet<Map<String, String>>(
        executeSelectQuery(queryMinus, Arrays.asList("s", "p", "o")));

    logger.info(getMessage(data, queryMinus, actualOutputMinus, expectedOutputMinus));
    assertEquals(expectedOutputMinus, actualOutputMinus);
  }

  /**
   * Constructs tested: SELECT, WHERE, FILTER NOT EXISTS/Minus(with Fixed Pattern)
   */
  public void test8_3_2() throws RDFParseException, RepositoryException, IOException {
    String data = "@prefix : <http://example/> .\n" + ":a :b :c .";
    ingestData(data);
    String queryFilterNotExists = "PREFIX : <http://example/>\n" + "SELECT * \n" + "{ \n"
        + "  ?s ?p ?o \n" + "  FILTER NOT EXISTS { :a :b :c }\n" + "}";
    Set<Map<String, String>> expectedOutputFilterNotExists = new HashSet<Map<String, String>>();
    Set<Map<String, String>> actualOutputFilterNotExists = new HashSet<Map<String, String>>(
        executeSelectQuery(queryFilterNotExists, Arrays.asList("s", "p", "o")));

    logger.info(getMessage(data, queryFilterNotExists, actualOutputFilterNotExists,
        expectedOutputFilterNotExists));
    assertEquals(expectedOutputFilterNotExists, actualOutputFilterNotExists);

    String queryMinus = "PREFIX : <http://example/>\n" + "SELECT * \n" + "{ \n" + "  ?s ?p ?o \n"
        + "  MINUS { :a :b :c }\n" + "}";
    Set<Map<String, String>> expectedOutputMinus = new HashSet<Map<String, String>>();
    expectedOutputMinus.add(
        ImmutableMap.of("s", "http://example/a", "p", "http://example/b", "o", "http://example/c"));
    Set<Map<String, String>> actualOutputMinus = new HashSet<Map<String, String>>(
        executeSelectQuery(queryMinus, Arrays.asList("s", "p", "o")));

    logger.info(getMessage(data, queryMinus, actualOutputMinus, expectedOutputMinus));
    assertEquals(expectedOutputMinus, actualOutputMinus);
  }

  /**
   * Constructs tested: SELECT, WHERE, FILTER NOT EXISTS/MINUS(with inner filters)
   */
  public void test8_3_3() throws RDFParseException, RepositoryException, IOException {
    String data = "@prefix : <http://example.com/> .\n" + ":a :p 1 .\n" + ":a :q 1 .\n"
        + ":a :q 2 .\n" + "\n" + ":b :p 3 .\n" + ":b :q 4 .\n" + ":b :q 5 .";
    String query = "PREFIX : <http://example.com/>\n" + "SELECT * WHERE {\n" + "        ?x :p ?n\n"
        + "        FILTER NOT EXISTS {\n" + "                ?x :q ?m .\n"
        + "                FILTER(?n = ?m)\n" + "        }\n" + "}";
    Set<Map<String, String>> expectedOutput = new HashSet<Map<String, String>>();
    expectedOutput.add(ImmutableMap.of("x", "http://example.com/b", "n", "3"));
    ingestData(data);
    Set<Map<String, String>> actualOutput =
        new HashSet<Map<String, String>>(executeSelectQuery(query, Arrays.asList("x", "n")));

    logger.info(getMessage(data, query, actualOutput, expectedOutput));
    assertEquals(expectedOutput, actualOutput);

  }

  /**
   * Constructs Tested: SELECT, WHERE, Property Path-Sequence Path
   */
  public void test9_3_a() throws RDFParseException, RepositoryException, IOException {
    String data = "@prefix :       <http://example/> .\n" + "\n" + ":order  :item :z1 .\n"
        + ":order  :item :z2 .\n" + "\n" + ":z1 :name \"Small\" .\n" + ":z1 :price 5 .\n" + "\n"
        + ":z2 :name \"Large\" .\n" + ":z2 :price 5 .";
    String query = "PREFIX :   <http://example/>\n" + "SELECT * \n" + "{  ?s :item/:price ?x . }";
    Set<Map<String, String>> expectedOutput = new HashSet<Map<String, String>>();
    expectedOutput.add(ImmutableMap.of("s", "http://example/order", "x", "5"));
    expectedOutput.add(ImmutableMap.of("s", "http://example/order", "x", "5"));

    ingestData(data);
    Set<Map<String, String>> actualOutput =
        new HashSet<Map<String, String>>(executeSelectQuery(query, Arrays.asList("s", "x")));

    logger.info(getMessage(data, query, actualOutput, expectedOutput));
    assertEquals(expectedOutput, actualOutput);

  }

  /**
   * Constructs Tested: SELECT, WHERE, SUM
   */
  public void test9_3_b() throws RDFParseException, RepositoryException, IOException {
    String data = "@prefix :       <http://example/> .\n" + "\n" + ":order  :item :z1 .\n"
        + ":order  :item :z2 .\n" + "\n" + ":z1 :name \"Small\" .\n" + ":z1 :price 5 .\n" + "\n"
        + ":z2 :name \"Large\" .\n" + ":z2 :price 5 .";
    String query = "PREFIX :   <http://example/>\n" + "  SELECT (sum(?x) AS ?total)\n" + "  { \n"
        + "    :order :item/:price ?x\n" + "  }";
    Set<Map<String, String>> expectedOutput = new HashSet<Map<String, String>>();
    expectedOutput.add(ImmutableMap.of("total", "10"));

    ingestData(data);
    Set<Map<String, String>> actualOutput =
        new HashSet<Map<String, String>>(executeSelectQuery(query, Arrays.asList("total")));

    logger.info(getMessage(data, query, actualOutput, expectedOutput));
    assertEquals(expectedOutput, actualOutput);

  }

  /**
   * Constructs Tested: SELECT, WHERE, VALUES
   */
  public void test10_2_2() throws RDFParseException, RepositoryException, IOException {
    String data = "@prefix dc:   <http://purl.org/dc/elements/1.1/> .\n"
        + "@prefix :     <http://example.org/book/> .\n"
        + "@prefix ns:   <http://example.org/ns#> .\n" + "\n"
        + ":book1  dc:title  \"SPARQL Tutorial\" .\n" + ":book1  ns:price  42 .\n"
        + ":book2  dc:title  \"The Semantic Web\" .\n" + ":book2  ns:price  23 .";
    String query = "PREFIX dc:   <http://purl.org/dc/elements/1.1/> \n"
        + "PREFIX :     <http://example.org/book/> \n" + "PREFIX ns:   <http://example.org/ns#> \n"
        + "\n" + "SELECT ?book ?title ?price\n" + "{\n" + "   ?book dc:title ?title ;\n"
        + "         ns:price ?price .\n" + "   VALUES (?book ?title)\n"
        + "   { (UNDEF \"SPARQL Tutorial\")\n" + "     (:book2 UNDEF)\n" + "   }\n" + "}";
    Set<Map<String, String>> expectedOutput = new HashSet<Map<String, String>>();
    expectedOutput.add(ImmutableMap.of("book", "http://example.org/book/book1", "title",
        "SPARQL Tutorial", "price", "42"));
    expectedOutput.add(ImmutableMap.of("book", "http://example.org/book/book2", "title",
        "The Semantic Web", "price", "23"));

    ingestData(data);
    Set<Map<String, String>> actualOutput = new HashSet<Map<String, String>>(
        executeSelectQuery(query, Arrays.asList("book", "title", "price")));

    logger.info(getMessage(data, query, actualOutput, expectedOutput));
    assertEquals(expectedOutput, actualOutput);

  }

  /**
   * Constructs Tested: SELECT, WHERE, GROUP BY,SUM, HAVING
   */
  public void test11_1() throws RDFParseException, RepositoryException, IOException {
    String data = "@prefix : <http://books.example/> .\n" + "\n"
        + ":org1 :affiliates :auth1, :auth2 .\n" + ":auth1 :writesBook :book1, :book2 .\n"
        + ":book1 :price 9 .\n" + ":book2 :price 5 .\n" + ":auth2 :writesBook :book3 .\n"
        + ":book3 :price 7 .\n" + ":org2 :affiliates :auth3 .\n" + ":auth3 :writesBook :book4 .\n"
        + ":book4 :price 7 .";
    String query = "PREFIX : <http://books.example/>\n" + "SELECT (SUM(?lprice) AS ?totalPrice)\n"
        + "WHERE {\n" + "  ?org :affiliates ?auth .\n" + "  ?auth :writesBook ?book .\n"
        + "  ?book :price ?lprice .\n" + "}\n" + "GROUP BY ?org\n" + "HAVING (SUM(?lprice) > 10)\n"
        + "";
    Set<Map<String, String>> expectedOutput = new HashSet<Map<String, String>>();
    expectedOutput.add(ImmutableMap.of("totalPrice", "21"));

    ingestData(data);
    Set<Map<String, String>> actualOutput =
        new HashSet<Map<String, String>>(executeSelectQuery(query, Arrays.asList("totalPrice")));

    logger.info(getMessage(data, query, actualOutput, expectedOutput));
    assertEquals(expectedOutput, actualOutput);
  }

  /**
   * Constructs Tested: SELECT, AS, GROUP BY
   */
  public void test12() throws RDFParseException, RepositoryException, IOException {
    String data = "@prefix : <http://people.example/> .\n" + "\n"
        + ":alice :name \"Alice\", \"Alice Foo\", \"A. Foo\" .\n" + ":alice :knows :bob, :carol .\n"
        + ":bob :name \"Bob\", \"Bob Bar\", \"B. Bar\" .\n"
        + ":carol :name \"Carol\", \"Carol Baz\", \"C. Baz\" .";
    String query =
        "PREFIX : <http://people.example/>\n" + "    SELECT ?y (MIN(?name) AS ?minName)\n"
            + "    WHERE {\n" + "      ?y :name ?name .\n" + "    } GROUP BY ?y\n";
    Set<Map<String, String>> expectedOutput = new HashSet<Map<String, String>>();
    expectedOutput.add(ImmutableMap.of("y", "http://people.example/bob", "minName", "B. Bar"));
    expectedOutput.add(ImmutableMap.of("y", "http://people.example/carol", "minName", "C. Baz"));
    expectedOutput.add(ImmutableMap.of("y", "http://people.example/alice", "minName", "A. Foo"));

    ingestData(data);
    Set<Map<String, String>> actualOutput =
        new HashSet<Map<String, String>>(executeSelectQuery(query, Arrays.asList("y", "minName")));

    logger.info(getMessage(data, query, actualOutput, expectedOutput));
    assertEquals(expectedOutput, actualOutput);
  }

  /**
   * Constructs Tested: SELECT, WHERE, DISTINCT
   */
  public void test15_3_1() throws RDFParseException, RepositoryException, IOException {
    String data = "@prefix  foaf:  <http://xmlns.com/foaf/0.1/> .\n" + "\n"
        + "_:x    foaf:name   \"Alice\" .\n" + "_:x    foaf:mbox   <mailto:alice@example.com> .\n"
        + "\n" + "_:y    foaf:name   \"Alice\" .\n"
        + "_:y    foaf:mbox   <mailto:asmith@example.com> .\n" + "\n"
        + "_:z    foaf:name   \"Alice\" .\n"
        + "_:z    foaf:mbox   <mailto:alice.smith@example.com> .";
    String query = "PREFIX foaf:    <http://xmlns.com/foaf/0.1/>\n"
        + "SELECT DISTINCT ?name WHERE { ?x foaf:name ?name }";
    Set<Map<String, String>> expectedOutput = new HashSet<Map<String, String>>();
    expectedOutput.add(ImmutableMap.of("name", "Alice"));

    ingestData(data);
    Set<Map<String, String>> actualOutput =
        new HashSet<Map<String, String>>(executeSelectQuery(query, Arrays.asList("name")));

    logger.info(getMessage(data, query, actualOutput, expectedOutput));
    assertEquals(expectedOutput, actualOutput);
  }

  /**
   * Constructs Tested: SELECT, AS
   */
  public void test16_1_2() throws RDFParseException, RepositoryException, IOException {
    String data = "@prefix dc: <http://purl.org/dc/elements/1.1/> .\n"
        + "@prefix : <http://example.org/book/> .\n" + "@prefix ns:<http://example.org/ns#> .\n"
        + "\n" + ":book1 dc:title \"SPARQL Tutorial\" .\n" + ":book1 ns:price 42 .\n"
        + ":book1 ns:discount 0.2 .\n" + "\n" + ":book2 dc:title \"The SemanticWeb\" .\n"
        + ":book2 ns:price 23 .\n" + ":book2 ns:discount 0.25 .";
    String query =
        "PREFIX dc: <http://purl.org/dc/elements/1.1/>\n" + "PREFIX ns: <http://example.org/ns#>\n"
            + "SELECT ?title (?p AS ?fullPrice) (?fullPrice*(1-?discount) AS ?customerPrice)\n"
            + "{ ?x ns:price ?p .\n" + " ?x dc:title ?title . \n" + " ?x ns:discount ?discount \n"
            + "}";

    Set<Map<String, String>> expectedOutput = new HashSet<Map<String, String>>();
    expectedOutput.add(
        ImmutableMap.of("title", "The SemanticWeb", "fullPrice", "23", "customerPrice", "17.25"));
    expectedOutput.add(
        ImmutableMap.of("title", "SPARQL Tutorial", "fullPrice", "42", "customerPrice", "33.60"));

    ingestData(data);
    Set<Map<String, String>> actualOutput = new HashSet<Map<String, String>>(
        executeSelectQuery(query, Arrays.asList("title", "fullPrice", "customerPrice")));

    logger.info(getMessage(data, query, actualOutput, expectedOutput));
    assertEquals(expectedOutput, actualOutput);

  }

  /*
   * Constructs Tested:SELECT,WHERE,ABS,FLOOR,CEIL,RAND,ROUND
   */
  public void test17_4_4() throws RDFParseException, RepositoryException, IOException {
    String data = "@prefix : <http://books.example/> .\n" + "\n"
        + ":org1 :affiliates :auth1, :auth2 .\n" + ":auth1 :writesBook :book1, :book2 .\n"
        + ":book1 :rating -2.5 .\n" + ":book2 :rating 2.5 .\n" + ":auth2 :writesBook :book3 .\n"
        + ":book3 :rating 2.499 .\n" + ":org2 :affiliates :auth3 .\n"
        + ":auth3 :writesBook :book4 .\n" + ":book4 :rating 0 .";
    String query = "PREFIX : <http://books.example/>\n"
        + "SELECT ?rating (FLOOR(?rating) AS ?floor) " + "(CEIL(?rating) AS ?ceil) "
        + "(ABS(?rating) AS ?abs) " + "(ROUND(?rating) AS ?round) " + "(rand() AS ?rand) "
        + "WHERE {\n" + "  ?book :rating ?rating .\n" + "}\n";
    Set<Map<String, String>> expectedOutput = new HashSet<Map<String, String>>();
    expectedOutput.add(ImmutableMap.of("abs", "2.50", "round", "-2.00", "rating", "-2.50", "ceil",
        "-2.00", "floor", "-3.00"));
    expectedOutput.add(ImmutableMap.of("abs", "0.00", "round", "0.00", "rating", "0.00", "ceil",
        "0.00", "floor", "0.00"));
    expectedOutput.add(ImmutableMap.of("abs", "2.50", "round", "3.00", "rating", "2.50", "ceil",
        "3.00", "floor", "2.00"));
    expectedOutput.add(ImmutableMap.of("abs", "2.50", "round", "2.00", "rating", "2.50", "ceil",
        "3.00", "floor", "2.00"));
    ingestData(data);
    Set<Map<String, String>> actualOutput = new HashSet<Map<String, String>>(
        executeSelectQuery(query, Arrays.asList("rating", "floor", "ceil", "abs", "round")));

    logger.info(getMessage(data, query, actualOutput, expectedOutput));
    assertEquals(expectedOutput, actualOutput);

  }

}
