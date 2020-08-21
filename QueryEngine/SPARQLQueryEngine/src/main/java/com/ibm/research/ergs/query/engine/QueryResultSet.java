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
package com.ibm.research.ergs.query.engine;

import java.util.Set;
import java.util.stream.Stream;

/**
 * This class is responsible for maintaining the result set of the query
 *
 * @author Udit Sharma
 *
 */
public class QueryResultSet {
  private Stream resultStream;
  private Set<String> queryVariables;

  /**
   * Constructs {@link QueryResultSet}
   *
   * @param resultStream {@link Stream} containing result for the query
   * @param queryVariables {@link Set} of variables in query
   */
  public QueryResultSet(Stream resultStream, Set<String> queryVariables) {
    this.resultStream = resultStream;
    this.queryVariables = queryVariables;
  }

  /**
   *
   * @return query result stream
   */
  public Stream getResultStream() {
    return resultStream;
  }

  /**
   *
   * @return {@link Set} of variables in query
   */
  public Set<String> getQueryVariables() {
    return queryVariables;
  }

}
