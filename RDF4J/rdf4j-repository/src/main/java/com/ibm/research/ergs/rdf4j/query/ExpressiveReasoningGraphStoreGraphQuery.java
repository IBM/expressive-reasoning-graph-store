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
package com.ibm.research.ergs.rdf4j.query;

import org.eclipse.rdf4j.query.GraphQuery;
import org.eclipse.rdf4j.query.GraphQueryResult;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import com.ibm.research.ergs.query.engine.QueryResultSet;
import com.ibm.research.ergs.query.engine.SPARQLQueryProcessor;

/**
 * SPARQL CONSTRUCT/DESCRIBE query for ERGS repository
 *
 * @author Udit-Sharma
 *
 */
public class ExpressiveReasoningGraphStoreGraphQuery extends AbstractExpressiveReasoningGraphStoreQuery implements GraphQuery {
  /**
   * It constructs SPARQL CONSTRUCT/DESCRIBE query for ERGS repository
   *
   * @param queryProcessor query execution engine
   * @param queryString query string
   * @param baseIRI base IRI for the query
   */
  public ExpressiveReasoningGraphStoreGraphQuery(SPARQLQueryProcessor queryProcessor, String queryString,
      String baseIRI) {
    super(queryProcessor, queryString, baseIRI);
  }

  @Override
  public void setMaxExecutionTime(int maxExecTime) {
    throw new UnsupportedOperationException("not implemented");

  }

  @Override
  public int getMaxExecutionTime() {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public GraphQueryResult evaluate() throws QueryEvaluationException {
    QueryResultSet result = queryProcessor.executeQuery(queryString);
    return new ExpressiveReasoningGraphStoreGraphQueryResult(result.getResultStream());
  }

  @Override
  public void evaluate(RDFHandler handler) throws QueryEvaluationException, RDFHandlerException {
    try (GraphQueryResult result = evaluate()) {
      handler.startRDF();
      while (result.hasNext()) {
        handler.handleStatement(result.next());
      }
      handler.endRDF();
    }

  }

}
