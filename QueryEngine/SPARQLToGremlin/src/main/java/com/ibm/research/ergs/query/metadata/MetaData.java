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
package com.ibm.research.ergs.query.metadata;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import com.ibm.research.ergs.query.metadata.IndexedFilterMetaData.IndexedFilter;
import com.ibm.research.ergs.query.metadata.PropertyTypeMetaData.PropertyType;
import com.ibm.research.ergs.query.metadata.VariableTypeMetaData.VariableType;
import com.ibm.research.ergs.query.utils.CostModel;
import com.ibm.research.ergs.query.utils.PropertyMapping;

/**
 * It contains common meta-data information for SPARQL query required during the query translation
 * process
 *
 * @author Udit Sharma
 *
 */
public class MetaData {
  private CostModel costModel;
  private PropertyMapping propertyMapping;
  // Stores filters utilizing indexes
  private IndexedFilterMetaData indexFilterMetadata;

  // Type of variables present in the query
  private VariableTypeMetaData variableTypeMetadata;

  // Set of variables present in the tupleExpr
  private VariablesMetaData variablesMetadata;

  // Statement generating a literal Var
  private TriplesMetaData triplesMetadata;

  // Types of properties and indexes in them
  private PropertyTypeMetaData propertyTypeMetadata;

  public MetaData(TupleExpr root, PropertyMapping propertyMapping, CostModel costModel) {
    this.propertyMapping = propertyMapping;
    this.costModel = costModel;
    this.propertyTypeMetadata = new PropertyTypeMetaData(root, this.propertyMapping);
    this.triplesMetadata = new TriplesMetaData(root, propertyTypeMetadata);
    this.variableTypeMetadata = new VariableTypeMetaData(root, propertyTypeMetadata);
    this.variablesMetadata = new VariablesMetaData(root, variableTypeMetadata);
    this.indexFilterMetadata = new IndexedFilterMetaData(root, costModel, propertyTypeMetadata,
        variablesMetadata, variableTypeMetadata, triplesMetadata);
  }

  /**
   * Checks whether property exist in the DB
   *
   * @param property predicate IRI
   * @return True if property is present false otherwise
   */
  public boolean isProperty(String property) {
    return propertyTypeMetadata.isProperty(property);
  }

  /**
   *
   * @param property predicate IRI
   * @return {@link PropertyType} for given property
   */
  public PropertyType extractPropertyType(String property) {
    return propertyTypeMetadata.extractPropertyType(property);
  }

  /**
   * Checks whether property is indexed in the DB
   *
   * @param property predicate IRI
   * @return True if property is indexed false otherwise
   */
  public boolean isIndexed(String property) {
    return propertyTypeMetadata.isIndexed(property);
  }

  /**
   * returns the variable names present in given {@link TupleExpr}
   *
   * @param tupleExpr expression
   * @return {@link Set} of variables in given expression
   */
  public Set<String> extractVariablesName(TupleExpr tupleExpr) {
    return extractVariables(tupleExpr).stream().map(var -> var.getName())
        .collect(Collectors.toSet());
  }

  /**
   * returns the variables present in given {@link TupleExpr}
   *
   * @param tupleExpr expression
   * @return {@link Set} of variables in given expression
   */
  public Set<Var> extractVariables(TupleExpr tupleExpr) {
    return variablesMetadata.extractVariables(tupleExpr);
  }

  /**
   * Returns the type of variable
   *
   * @param variableName name of variable
   * @return {@link VariableType} for variable
   */
  public VariableType getVariableType(String variableName) {
    return variableTypeMetadata.getVariableType(variableName);
  }

  /**
   * Returns the {@link List} of {@link IndexedFilter} for given {@link TupleExpr}
   *
   * @param tupleExpr expression
   * @return indexed filters for given expression
   */
  public List<IndexedFilter> getIndexedFilter(TupleExpr tupleExpr) {
    return indexFilterMetadata.getIndexedFilter(tupleExpr);
  }

  /**
   * returns true if a has better indexes than b
   *
   * @param a first TupleExpr
   * @param b second TupleExpr
   * @return true if either there is no indexedFilter available on b or there are better
   *         indexedFitler available for a, false otherwise
   */
  public boolean isBetterIndexed(TupleExpr a, TupleExpr b) {
    return indexFilterMetadata.isBetterIndexed(a, b);
  }

  /**
   * Get triple having Variable var
   *
   * @param var variable
   * @return {@link StatementPattern}
   */
  public StatementPattern getTriples(Var var) {
    return triplesMetadata.getTriples(var);
  }

  public long getRelationshipRangeCardinalityForFixedDomain(String edgeLabel) {
    return costModel.getRelationshipRangeCardinalityForFixedDomain(edgeLabel);
  }

  public long getRelationshipDomainCardinalityForFixedRange(String edgeLabel) {
    return costModel.getRelationshipDomainCardinalityForFixedRange(edgeLabel);
  }

}
