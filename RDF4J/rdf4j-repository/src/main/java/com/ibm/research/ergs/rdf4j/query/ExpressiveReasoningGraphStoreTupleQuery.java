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

import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.TupleQueryResultHandler;
import org.eclipse.rdf4j.query.TupleQueryResultHandlerException;
import com.ibm.research.ergs.query.engine.QueryResultSet;
import com.ibm.research.ergs.query.engine.SPARQLQueryProcessor;

/**
 * SPARQL SELECT query for ERGS repository
 *
 * @author Udit Sharma
 *
 */
public class ExpressiveReasoningGraphStoreTupleQuery
    extends AbstractExpressiveReasoningGraphStoreQuery implements TupleQuery {

  /**
   * It constructs SPARQL SELECT query for ERGS repository
   *
   * @param queryProcessor query execution engine
   * @param queryString query string
   * @param baseIRI base IRI for the query
   */
  public ExpressiveReasoningGraphStoreTupleQuery(SPARQLQueryProcessor queryProcessor,
      String queryString, String baseIRI) {
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
  public TupleQueryResult evaluate() throws QueryEvaluationException {
    QueryResultSet result = queryProcessor.executeQuery(queryString);
    return new ExpressiveReasoningGraphStoreTupleQueryResult(result.getResultStream(),
        result.getQueryVariables());

  }

  @Override
  public void evaluate(TupleQueryResultHandler handler)
      throws QueryEvaluationException, TupleQueryResultHandlerException {
    TupleQueryResult result = evaluate();
    handler.startQueryResult(result.getBindingNames());
    while (result.hasNext()) {
      handler.handleSolution(result.next());
    }
    handler.endQueryResult();
  }

}
