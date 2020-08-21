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

import org.eclipse.rdf4j.query.BooleanQuery;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import com.ibm.research.ergs.query.engine.QueryResultSet;
import com.ibm.research.ergs.query.engine.SPARQLQueryProcessor;

/**
 * SPARQL ASK query for ERGS repository
 *
 * @author Udit Sharma
 *
 */
public class ExpressiveReasoningGraphStoreBooleanQuery extends AbstractExpressiveReasoningGraphStoreQuery implements BooleanQuery {

  /**
   * It constructs SPARQL ASK query for ERGS repository
   *
   * @param queryProcessor query execution engine
   * @param queryString query string
   * @param baseIRI base IRI for the query
   */
  public ExpressiveReasoningGraphStoreBooleanQuery(SPARQLQueryProcessor queryProcessor, String queryString,
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
  public boolean evaluate() throws QueryEvaluationException {
    QueryResultSet result = queryProcessor.executeQuery(queryString);
    Iterable<String> it =
        (Iterable<String>) result.getResultStream().map(Object::toString)::iterator;
    return it.iterator().hasNext();
  }

}
