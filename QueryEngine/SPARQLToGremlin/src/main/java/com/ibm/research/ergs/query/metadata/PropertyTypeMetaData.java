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

import java.util.HashMap;
import java.util.Map;
import org.eclipse.rdf4j.query.algebra.ArbitraryLengthPath;
import org.eclipse.rdf4j.query.algebra.BinaryTupleOperator;
import org.eclipse.rdf4j.query.algebra.BinaryValueOperator;
import org.eclipse.rdf4j.query.algebra.Filter;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.SubQueryValueOperator;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.UnaryTupleOperator;
import org.eclipse.rdf4j.query.algebra.UnaryValueOperator;
import org.eclipse.rdf4j.query.algebra.ValueConstant;
import org.eclipse.rdf4j.query.algebra.ValueExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.ibm.research.ergs.query.utils.PropertyMapping;
import com.ibm.research.ergs.query.utils.RDF4JHelper;

/**
 * This class is responsible for extracting predicate type and predicate index availability for each
 * predicate used in the query
 *
 * @author Udit Sharma
 *
 */
public class PropertyTypeMetaData {
  private static final Logger logger = LoggerFactory.getLogger(PropertyTypeMetaData.class);

  private Map<String, PropertyType> propertyTypeMap;
  private Map<String, Boolean> indexMap;
  PropertyMapping propertyMapping;

  public PropertyTypeMetaData(TupleExpr root, PropertyMapping propertyMapping) {
    this.propertyTypeMap = new HashMap<String, PropertyType>();
    this.indexMap = new HashMap<String, Boolean>();
    this.propertyMapping = propertyMapping;
    process(root);
    // System.out.println(propertyTypeMap);
    // System.out.println(indexMap);
  }

  /**
   * Checks whether property exist in the DB
   *
   * @param property predicate IRI
   * @return True if property is present false otherwise
   */
  public boolean isProperty(String property) {
    return propertyTypeMap.containsKey(property);
  }

  /**
   *
   * @param property predicate IRI
   * @return {@link PropertyType} for given property
   */
  public PropertyType extractPropertyType(String property) {
    return propertyTypeMap.get(property);
  }

  /**
   * Checks whether property is indexed in the DB
   *
   * @param property predicate IRI
   * @return True if property is indexed false otherwise
   */
  public boolean isIndexed(String property) {
    return indexMap.get(property);
  }

  public static enum PropertyType {
    DATATYPE_PROPERTY, OBJECT_PROPERTY, CONFLICTING_PROPERTY;
  }

  /**
   *
   * Populates propertyTypeMap with type of property and indexMap with index availability flag for
   * given {@link TupleExpr} expression
   *
   * @param tupleExpr
   */
  private void process(TupleExpr tupleExpr) {
    if (tupleExpr instanceof Filter) {
      process((Filter) tupleExpr);
    } else if (tupleExpr instanceof UnaryTupleOperator) {
      process((UnaryTupleOperator) tupleExpr);
    } else if (tupleExpr instanceof BinaryTupleOperator) {
      process((BinaryTupleOperator) tupleExpr);
    } else if (tupleExpr instanceof StatementPattern) {
      process((StatementPattern) tupleExpr);
    } else if (tupleExpr instanceof ArbitraryLengthPath) {
      process((ArbitraryLengthPath) tupleExpr);
      // TODO: Add Extension(Bind) and BindingSetAssignment(Values)
    } else {
      logger.warn("unsupported expression" + tupleExpr);
    }
  }

  /**
   *
   * Populates propertyTypeMap with type of property and indexMap with index availability flag for
   * given {@link ValueExpr} expression
   *
   * @param valueExpr
   */
  private void process(ValueExpr valueExpr) {
    if (valueExpr instanceof SubQueryValueOperator) {
      process((SubQueryValueOperator) valueExpr);
    } else if (valueExpr instanceof UnaryValueOperator) {
      process((UnaryValueOperator) valueExpr);
    } else if (valueExpr instanceof BinaryValueOperator) {
      process((BinaryValueOperator) valueExpr);
    } else if (valueExpr instanceof ValueConstant) {
      process((ValueConstant) valueExpr);
    } else {
      logger.warn("unsupported expression: " + valueExpr);
    }
  }

  /**
   *
   * Populates propertyTypeMap with type of property and indexMap with index availability flag for
   * given {@link ValueConstant} expression
   *
   * @param constant
   */
  private void process(ValueConstant constant) {
    Object value = RDF4JHelper.getValue(constant.getValue());
    // if value is String populate the maps with appropriate info
    if (value instanceof String) {
      String stringConstant = (String) value;
      process(stringConstant);
    }
  }

  /**
   *
   * Populates propertyTypeMap with type of property and indexMap with index availability flag for
   * given string
   *
   * @param stringConstant string containing property IRI
   */
  private void process(String stringConstant) {
    // Populate the propertyTypeMap with property type
    if (propertyMapping.isDataTypeProperty(stringConstant)) {
      propertyTypeMap.put(stringConstant, PropertyType.DATATYPE_PROPERTY);
    } else if (propertyMapping.isObjectProperty(stringConstant)) {
      propertyTypeMap.put(stringConstant, PropertyType.OBJECT_PROPERTY);
    } else if (propertyMapping.isConflictingProperty(stringConstant)) {
      propertyTypeMap.put(stringConstant, PropertyType.CONFLICTING_PROPERTY);
    }

    // Populate the indexMap with index availability flag
    if (propertyMapping.isIndexed(stringConstant)) {
      indexMap.put(stringConstant, true);
    } else {
      indexMap.put(stringConstant, false);
    }

  }

  /**
   *
   * Populates propertyTypeMap with type of property and indexMap with index availability flag for
   * given {@link BinaryValueOperator} expression
   *
   * @param valueExpr
   */
  private void process(BinaryValueOperator valueExpr) {
    process(valueExpr.getLeftArg());
    process(valueExpr.getRightArg());
  }

  /**
   *
   * Populates propertyTypeMap with type of property and indexMap with index availability flag for
   * given {@link UnaryValueOperator} expression
   *
   * @param valueExpr
   */
  private void process(UnaryValueOperator valueExpr) {
    process(valueExpr.getArg());
  }

  /**
   *
   * Populates propertyTypeMap with type of property and indexMap with index availability flag for
   * given {@link SubQueryValueOperator} expression
   *
   * @param valueExpr
   */
  private void process(SubQueryValueOperator valueExpr) {
    process(valueExpr.getSubQuery());
  }

  /**
   *
   * Populates propertyTypeMap with type of property and indexMap with index availability flag for
   * given {@link Filter} expression
   *
   * @param tupleExpr
   */
  private void process(Filter tupleExpr) {
    process(tupleExpr.getCondition());
    process(tupleExpr.getArg());
  }

  /**
   *
   * Populates propertyTypeMap with type of property and indexMap with index availability flag for
   * given {@link UnaryTupleOperator} expression
   *
   * @param tupleExpr
   */
  private void process(UnaryTupleOperator tupleExpr) {
    process(tupleExpr.getArg());
  }

  /**
   *
   * Populates propertyTypeMap with type of property and indexMap with index availability flag for
   * given {@link BinaryTupleOperator} expression
   *
   * @param tupleExpr
   */
  private void process(BinaryTupleOperator tupleExpr) {
    process(tupleExpr.getLeftArg());
    process(tupleExpr.getRightArg());
  }

  /**
   *
   * Populates propertyTypeMap with type of property and indexMap with index availability flag for
   * given {@link StatementPattern} expression
   *
   * @param tupleExpr
   */
  private void process(StatementPattern tupleExpr) {
    Var predicate = tupleExpr.getPredicateVar();
    // populate the maps with appropriate info for the predicate of triple
    if (RDF4JHelper.isConcrete(predicate)) {
      String stringConstant = predicate.getValue().stringValue();
      process(stringConstant);
    }
  }

  /**
   *
   * Populates propertyTypeMap with type of property and indexMap with index availability flag for
   * given {@link ArbitraryLengthPath} expression
   *
   * @param tupleExpr
   */
  private void process(ArbitraryLengthPath tupleExpr) {
    process(tupleExpr.getPathExpression());
  }

}
