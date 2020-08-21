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

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.algebra.ArbitraryLengthPath;
import org.eclipse.rdf4j.query.algebra.BinaryTupleOperator;
import org.eclipse.rdf4j.query.algebra.BinaryValueOperator;
import org.eclipse.rdf4j.query.algebra.BindingSetAssignment;
import org.eclipse.rdf4j.query.algebra.Extension;
import org.eclipse.rdf4j.query.algebra.ExtensionElem;
import org.eclipse.rdf4j.query.algebra.Filter;
import org.eclipse.rdf4j.query.algebra.LeftJoin;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.SubQueryValueOperator;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.UnaryTupleOperator;
import org.eclipse.rdf4j.query.algebra.UnaryValueOperator;
import org.eclipse.rdf4j.query.algebra.ValueConstant;
import org.eclipse.rdf4j.query.algebra.ValueExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.ZeroLengthPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.ibm.research.ergs.query.metadata.PropertyTypeMetaData.PropertyType;
import com.ibm.research.ergs.query.utils.RDF4JHelper;

/**
 * This class extracts variable types for all the variable in the query
 *
 * @author Udit Sharma
 *
 */
public class VariableTypeMetaData {
  private static final Logger logger = LoggerFactory.getLogger(VariableTypeMetaData.class);

  public static enum VariableType {
    NODE_IRI, PREDICATE_IRI, LITERAL, ANY;
  }

  private PropertyTypeMetaData propertyTypeMetadata;

  private Map<String, VariableType> variableTypeMap;

  /**
   * Constructs {@link VariableTypeMetaData}
   *
   * @param root root of query parse tree
   * @param propertyTypeMetadata {@link PropertyTypeMetaData} for properties in the query
   */
  public VariableTypeMetaData(TupleExpr root, PropertyTypeMetaData propertyTypeMetadata) {
    this.propertyTypeMetadata = propertyTypeMetadata;
    this.variableTypeMap = new HashMap<String, VariableType>();
    process(root, false);
    // System.out.println(variableTypeMap);

  }

  /**
   * Returns the type of variable
   *
   * @param variableName name of variable
   * @return {@link VariableType} for variable
   */
  public VariableType getVariableType(String variableName) {
    return variableTypeMap.get(variableName);
  }

  /**
   * Populates variableMap with {@link VariableType} for variables present in {@link TupleExpr}
   * expression
   *
   * @param tupleExpr
   * @param isOptional True if tupleExpr is part of optional construct, False otherwise
   */
  private void process(TupleExpr tupleExpr, boolean isOptional) {
    if (tupleExpr instanceof Filter) {
      process((Filter) tupleExpr, isOptional);
    } else if (tupleExpr instanceof Extension) {
      process((Extension) tupleExpr, isOptional);
    } else if (tupleExpr instanceof UnaryTupleOperator) {
      process((UnaryTupleOperator) tupleExpr, isOptional);
    } else if (tupleExpr instanceof LeftJoin) {
      process((LeftJoin) tupleExpr, isOptional);
    } else if (tupleExpr instanceof BinaryTupleOperator) {
      process((BinaryTupleOperator) tupleExpr, isOptional);
    } else if (tupleExpr instanceof StatementPattern) {
      process((StatementPattern) tupleExpr, isOptional);
    } else if (tupleExpr instanceof ArbitraryLengthPath) {
      process((ArbitraryLengthPath) tupleExpr, isOptional);
    } else if (tupleExpr instanceof ZeroLengthPath) {
      process((ZeroLengthPath) tupleExpr, isOptional);
    } else if (tupleExpr instanceof BindingSetAssignment) {
      process((BindingSetAssignment) tupleExpr, isOptional);
    } else {
      logger.warn("Unsupported expression: " + tupleExpr);
    }
  }

  /**
   * Checks whether string is valid url
   *
   * @param constant
   * @return True if string is IRI, False otherwise
   */
  private boolean isIRI(String constant) {
    try {
      new URL(constant).toURI();
      return true;
    } catch (URISyntaxException exception) {
      return false;
    } catch (MalformedURLException exception) {
      return false;
    }

  }

  /**
   * Populates variableMap with {@link VariableType} for variables present in {@link Extension}
   * expression
   *
   * @param tupleExpr
   * @param isOptional True if tupleExpr is part of optional construct, False otherwise
   */
  private void process(Extension tupleExpr, boolean isOptional) {
    process(tupleExpr.getArg(), isOptional);
    for (ExtensionElem elem : tupleExpr.getElements()) {
      String asVar = elem.getName();
      if (elem.getExpr() instanceof Var) {
        // if expression is variable use the type already populated
        String exprVar = ((Var) elem.getExpr()).getName();
        variableTypeMap.put(asVar, variableTypeMap.get(exprVar));
      } else if (elem.getExpr() instanceof ValueConstant) {
        String exprVar = ((ValueConstant) elem.getExpr()).getValue().stringValue();
        // if expression is a property
        if (propertyTypeMetadata.isProperty(exprVar)) {
          variableTypeMap.put(asVar, VariableType.PREDICATE_IRI);
        } else if (isIRI(exprVar)) {
          variableTypeMap.put(asVar, VariableType.NODE_IRI);
          // else it is a LIteral
        } else {
          variableTypeMap.put(asVar, VariableType.LITERAL);
        }
      } else {
        variableTypeMap.put(asVar, VariableType.LITERAL);
      }

    }
  }

  /**
   * Populates variableMap with {@link VariableType} for variables present in
   * {@link BindingSetAssignment} expression
   *
   * @param tupleExpr
   * @param isOptional True if tupleExpr is part of optional construct, False otherwise
   */
  private void process(BindingSetAssignment tupleExpr, boolean isOptional) {
    // collect bindings per variable
    Map<String, List<String>> bindingsList = new HashMap<String, List<String>>();
    for (String name : tupleExpr.getBindingNames()) {
      bindingsList.put(name, new ArrayList<String>());
    }
    for (BindingSet bs : tupleExpr.getBindingSets()) {
      for (String name : tupleExpr.getBindingNames()) {
        if (bs.getValue(name) != null) {
          bindingsList.get(name).add(bs.getValue(name).stringValue());
        }
      }
    }
    for (String name : bindingsList.keySet()) {
      // if all the bindings for a variable are property
      if (bindingsList.get(name).stream().allMatch(a -> propertyTypeMetadata.isProperty(a))) {
        variableTypeMap.put(name, VariableType.PREDICATE_IRI);
      }
      // if all the bindings for a variable are IRIs
      else if (bindingsList.get(name).stream().allMatch(a -> isIRI(a))) {
        variableTypeMap.put(name, VariableType.NODE_IRI);
      }
      // if all the bindings for a variable are Literals
      else if (bindingsList.get(name).stream().allMatch(a -> !isIRI(a))) {
        variableTypeMap.put(name, VariableType.LITERAL);
      }
    }
  }

  /**
   * Populates variableMap with {@link VariableType} for variables present in {@link Filter}
   * expression
   *
   * @param tupleExpr
   * @param isOptional True if tupleExpr is part of optional construct, False otherwise
   */
  private void process(Filter tupleExpr, boolean isOptional) {
    process(tupleExpr.getCondition());
    process(tupleExpr.getArg(), isOptional);
  }

  /**
   * Populates variableMap with {@link VariableType} for variables present in
   * {@link UnaryTupleOperator} expression
   *
   * @param tupleExpr
   * @param isOptional True if tupleExpr is part of optional construct, False otherwise
   */
  private void process(UnaryTupleOperator tupleExpr, boolean isOptional) {
    process(tupleExpr.getArg(), isOptional);
  }

  /**
   * Populates variableMap with {@link VariableType} for variables present in {@link LeftJoin}
   * expression
   *
   * @param tupleExpr
   * @param isOptional True if tupleExpr is part of optional construct, False otherwise
   */
  private void process(LeftJoin tupleExpr, boolean isOptional) {
    process(tupleExpr.getLeftArg(), isOptional);
    process(tupleExpr.getRightArg(), true);
  }

  /**
   * Populates variableMap with {@link VariableType} for variables present in
   * {@link BinaryTupleOperator} expression
   *
   * @param tupleExpr
   * @param isOptional True if tupleExpr is part of optional construct, False otherwise
   */
  private void process(BinaryTupleOperator tupleExpr, boolean isOptional) {
    process(tupleExpr.getLeftArg(), isOptional);
    process(tupleExpr.getRightArg(), isOptional);
  }

  /**
   * Populates variableMap with {@link VariableType} for variables present in
   * {@link StatementPattern} expression
   *
   * @param tupleExpr
   * @param isOptional True if tupleExpr is part of optional construct, False otherwise
   */
  private void process(StatementPattern tupleExpr, boolean isOptional) {
    Var subject = tupleExpr.getSubjectVar();
    Var predicate = tupleExpr.getPredicateVar();
    Var object = tupleExpr.getObjectVar();
    // Variable at subject position is NODE_IRI or PREDICATE_IRI
    if (RDF4JHelper.isVariable(subject)) {
      if (!variableTypeMap.containsKey(subject.getName())) {
        variableTypeMap.put(subject.getName(), VariableType.NODE_IRI);
      } else if (!isOptional && variableTypeMap.get(subject.getName()) == VariableType.ANY) {
        variableTypeMap.put(subject.getName(), VariableType.NODE_IRI);
      }
    }
    // Variable at predicate position is PREDICATE_IRI
    if (RDF4JHelper.isVariable(predicate)) {
      variableTypeMap.put(predicate.getName(), VariableType.PREDICATE_IRI);
    }
    // Variable at object position could be LITERAL or NODE_IRI
    if (RDF4JHelper.isVariable(object)) {
      if (RDF4JHelper.isVariable(predicate) && !variableTypeMap.containsKey(object.getName())) {
        variableTypeMap.put(object.getName(), VariableType.ANY);
      } else if (RDF4JHelper.isConcrete(predicate)) {
        if (propertyTypeMetadata.extractPropertyType(
            predicate.getValue().stringValue()) == PropertyType.DATATYPE_PROPERTY) {
          variableTypeMap.put(object.getName(), VariableType.LITERAL);
        } else if (propertyTypeMetadata.extractPropertyType(
            predicate.getValue().stringValue()) == PropertyType.OBJECT_PROPERTY) {
          variableTypeMap.put(object.getName(), VariableType.NODE_IRI);
        } else if (!variableTypeMap.containsKey(object.getName())) {
          variableTypeMap.put(object.getName(), VariableType.ANY);
        }
      }
    }
  }

  /**
   * Populates variableMap with {@link VariableType} for variables present in
   * {@link ArbitraryLengthPath} expression
   *
   * @param tupleExpr
   * @param isOptional True if tupleExpr is part of optional construct, False otherwise
   */
  private void process(ArbitraryLengthPath tupleExpr, boolean isOptional) {
    process(tupleExpr.getPathExpression(), isOptional);
  }

  /**
   * Populates variableMap with {@link VariableType} for variables present in {@link ZeroLengthPath}
   * expression
   *
   * @param tupleExpr
   * @param isOptional True if tupleExpr is part of optional construct, False otherwise
   */
  private void process(ZeroLengthPath tupleExpr, boolean isOptional) {
    Var subject = tupleExpr.getSubjectVar();
    Var object = tupleExpr.getObjectVar();
    if (RDF4JHelper.isVariable(subject) && !variableTypeMap.containsKey(subject.getName())) {
      variableTypeMap.put(subject.getName(), VariableType.NODE_IRI);
    }
    if (RDF4JHelper.isVariable(object)) {
      variableTypeMap.put(object.getName(), VariableType.NODE_IRI);
    }
  }

  /**
   * Populates variableMap with {@link VariableType} for variables present in {@link ValueExpr}
   * expression
   *
   * @param tupleExpr
   */
  private void process(ValueExpr valueExpr) {
    if (valueExpr instanceof SubQueryValueOperator) {
      process(((SubQueryValueOperator) valueExpr));
    } else if (valueExpr instanceof UnaryValueOperator) {
      process(((UnaryValueOperator) valueExpr));
    } else if (valueExpr instanceof BinaryValueOperator) {
      process(((BinaryValueOperator) valueExpr));
    }
  }

  /**
   * Populates variableMap with {@link VariableType} for variables present in
   * {@link BinaryValueOperator} expression
   *
   * @param tupleExpr
   */
  private void process(BinaryValueOperator valueExpr) {
    process(valueExpr.getLeftArg());
    process(valueExpr.getRightArg());
  }

  /**
   * Populates variableMap with {@link VariableType} for variables present in
   * {@link UnaryValueOperator} expression
   *
   * @param tupleExpr
   */
  private void process(UnaryValueOperator valueExpr) {
    process(valueExpr.getArg());
  }

  /**
   * Populates variableMap with {@link VariableType} for variables present in
   * {@link SubQueryValueOperator} expression
   *
   * @param tupleExpr
   */
  private void process(SubQueryValueOperator valueExpr) {
    process(valueExpr.getSubQuery(), false);
  }

}
