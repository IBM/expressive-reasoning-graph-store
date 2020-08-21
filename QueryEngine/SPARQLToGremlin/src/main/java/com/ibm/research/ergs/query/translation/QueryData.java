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
package com.ibm.research.ergs.query.translation;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.eclipse.rdf4j.query.algebra.AggregateOperator;
import org.eclipse.rdf4j.query.algebra.Var;
import com.ibm.research.ergs.query.metadata.MetaData;

/**
 * It holds the data specific to a query which is used during query translation
 *
 * @author Udit Sharma
 *
 */
public class QueryData {

  private int counter = 0;
  // Mapping of anonymous variable to variable present in the query
  private HashMap<Var, String> variableMapping;
  // Set of variables in query
  private Set<String> queryVariables;
  private MetaData queryMetadata;
  private Map<AggregateOperator, String> aliasForaggregators;
  private GenericQueryConversion queryConvertor;

  /**
   * Constructs {@link QueryData}
   *
   * @param queryMetadata {@link MetaData} for quey
   * @param queryConvertor {@link GenericQueryConversion} for query
   */
  public QueryData(MetaData queryMetadata, GenericQueryConversion queryConvertor) {
    this.counter = 0;
    this.variableMapping = new HashMap<Var, String>();
    this.queryVariables = new HashSet<String>();
    this.aliasForaggregators = new HashMap<AggregateOperator, String>();
    this.queryMetadata = queryMetadata;
    this.queryConvertor = queryConvertor;
  }

  /**
   * It adds variable to the {@link Set} Of query variables
   *
   * @param variables {@link Collection} of the variables to add
   */
  public void addQueryVariable(Collection<String> variables) {
    this.queryVariables.addAll(variables);
  }

  /**
   * It adds a single variable to the {@link Set} Of query variables
   *
   * @param variable variable to add
   */
  public void addQueryVariable(String variable) {
    this.queryVariables.add(variable);
  }

  /**
   * Add alias for aggregate operator
   *
   * @param operator aggregate
   * @param alias alias for aggregate
   */
  public void addAlias(AggregateOperator operator, String alias) {
    this.aliasForaggregators.put(operator, alias);
  }

  /**
   *
   * @return {@link MetaData} for query
   */
  public MetaData getQueryMetaData() {
    return queryMetadata;
  }

  /**
   *
   * @return variables in query
   */
  public Set<String> getQueryVariables() {
    return queryVariables;
  }

  /**
   *
   * @param operator {@link AggregateOperator}
   * @return alias for aggregate expression
   */
  public String getAlias(AggregateOperator operator) {
    return aliasForaggregators.get(operator);
  }

  /**
   *
   * @return {@link GenericQueryConversion} for query
   */
  public GenericQueryConversion getQueryConvertor() {
    return queryConvertor;
  }

  /**
   *
   * @return A new anonymous variable. Used during query translation
   */
  public String getAnonymousVariable() {
    counter++;
    return "temp" + counter;
  }

  /**
   *
   * @return anonymous variable mapped to vat
   */
  public String getAnonymousVariable(Var var) {
    if (variableMapping.get(var) == null) {
      variableMapping.put(var, getAnonymousVariable());
    }

    return variableMapping.get(var);
  }

}
