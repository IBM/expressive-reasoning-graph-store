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
package com.ibm.research.ergs.rdf4j.repository;

import java.io.IOException;
import org.eclipse.rdf4j.query.BooleanQuery;
import org.eclipse.rdf4j.query.GraphQuery;
import org.eclipse.rdf4j.query.GraphQueryResult;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.UnsupportedQueryLanguageException;
import org.eclipse.rdf4j.query.parser.ParsedBooleanQuery;
import org.eclipse.rdf4j.query.parser.ParsedGraphQuery;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.ParsedTupleQuery;
import org.eclipse.rdf4j.query.parser.QueryParserUtil;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.rio.RDFParseException;

/**
 * It is used for calculating cold and warm cache time for a given query
 * 
 * @author Udit Sharma
 *
 */
public class ComputeTime {
  private static int NUM_RUNS = 5;

  public static void main(String args[])
      throws RDFParseException, RepositoryException, IOException {
    if (args.length < 2) {
      System.out
          .println("Insufficient arguments. Following should be sructure of input arguments.");
      System.out.println("ComputeTime <repository_id> <query>");
      return;
    }
    String repository_id = args[0];
    String query = args[1];
    Repository repo = new ExpressiveReasoningGraphStoreRepository(repository_id, true);
    repo.init();
    try (RepositoryConnection conn = repo.getConnection()) {
      try {
        long a = System.currentTimeMillis();
        long size = executeSparqlQuery(query, conn);
        long b = System.currentTimeMillis();

        long c = System.currentTimeMillis();
        for (int j = 0; j < NUM_RUNS; j++) {
          executeSparqlQuery(query, conn);
        }
        long d = System.currentTimeMillis();

        System.out.println("Output Size: " + size);
        System.out.println("Cold Cache :" + (b - a));

        System.out.println("Warm Cache :" + (d - c) / NUM_RUNS);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    repo.shutDown();
  }

  /**
   * 
   * @param query SPARQL query string
   * @param conn Repository connection object
   * @return number of results for given query
   * @throws MalformedQueryException
   * @throws UnsupportedQueryLanguageException
   * @throws RepositoryException
   * @throws QueryEvaluationException
   */
  private static long executeSparqlQuery(String query, RepositoryConnection conn)
      throws MalformedQueryException, UnsupportedQueryLanguageException, RepositoryException,
      QueryEvaluationException {
    ParsedQuery q = QueryParserUtil.parseQuery(QueryLanguage.SPARQL, query, null);

    if (q instanceof ParsedTupleQuery) {
      return executeSelectQuery(query, conn);
    } else if (q instanceof ParsedBooleanQuery) {
      return executeAskQuery(query, conn);
    } else if (q instanceof ParsedGraphQuery) {
      return executeGraphQuery(query, conn);
    } else {
      throw new MalformedQueryException("Unrecognized query type. " + query);
    }
  }

  /**
   * 
   * @param query SPARQL SELECT query string
   * @param conn Repository connection object
   * @return number of results for given query
   * @throws RepositoryException
   * @throws MalformedQueryException
   * @throws QueryEvaluationException
   */
  private static long executeSelectQuery(String query, RepositoryConnection conn)
      throws RepositoryException, MalformedQueryException, QueryEvaluationException {
    TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
    TupleQueryResult result = tupleQuery.evaluate();

    long count = 0;
    try {
      while (result.hasNext()) { // iterate over the result
        result.next();
        count++;
      }
    } finally {
      result.close();
    }
    return count;
  }


  /**
   * 
   * @param query SPARQL ASK query string
   * @param conn Repository connection object
   * @return number of results for given query
   * @throws RepositoryException
   * @throws MalformedQueryException
   * @throws QueryEvaluationException
   */
  private static long executeAskQuery(String query, RepositoryConnection conn)
      throws RepositoryException, MalformedQueryException, QueryEvaluationException {
    BooleanQuery booleanQuery = conn.prepareBooleanQuery(QueryLanguage.SPARQL, query);
    return booleanQuery.evaluate() ? 1 : 0;
  }


  /**
   * 
   * @param query SPARQL DESCRIBE/CONSTRUCT query string
   * @param conn Repository connection object
   * @return number of results for given query
   * @throws RepositoryException
   * @throws MalformedQueryException
   * @throws QueryEvaluationException
   */
  private static long executeGraphQuery(String query, RepositoryConnection conn)
      throws QueryEvaluationException, RepositoryException, MalformedQueryException {
    GraphQuery graphQuery = conn.prepareGraphQuery(QueryLanguage.SPARQL, query);

    long count = 0;
    GraphQueryResult result = graphQuery.evaluate();
    try {
      while (result.hasNext()) {
        result.next();
        count++;
      }
    } finally {
      result.close();
    }
    return count;

  }

}
