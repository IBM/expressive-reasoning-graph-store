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

/**
 * It is used for executing a SPARQL query
 *
 * @author Udit Sharma
 *
 */
public interface SPARQLQueryProcessor {
  /**
   * Executes SPARQL query
   *
   * @param queryString {@link String} containing SPARQL query
   * @return result of SPARQL query
   */
  public QueryResultSet executeQuery(String queryString);
}
