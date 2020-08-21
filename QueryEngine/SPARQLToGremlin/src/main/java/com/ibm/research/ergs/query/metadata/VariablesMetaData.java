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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.eclipse.rdf4j.query.algebra.ArbitraryLengthPath;
import org.eclipse.rdf4j.query.algebra.BinaryTupleOperator;
import org.eclipse.rdf4j.query.algebra.BinaryValueOperator;
import org.eclipse.rdf4j.query.algebra.BindingSetAssignment;
import org.eclipse.rdf4j.query.algebra.Extension;
import org.eclipse.rdf4j.query.algebra.ExtensionElem;
import org.eclipse.rdf4j.query.algebra.Filter;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.SubQueryValueOperator;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.UnaryTupleOperator;
import org.eclipse.rdf4j.query.algebra.UnaryValueOperator;
import org.eclipse.rdf4j.query.algebra.ValueExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.ZeroLengthPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spark_project.guava.collect.Sets;
import com.ibm.research.ergs.query.utils.RDF4JHelper;

/**
 * This class is responsible for extracting the variables used in a TupleExpr
 *
 * @author Udit Sharma
 *
 */
public class VariablesMetaData {
  private static final Logger logger = LoggerFactory.getLogger(VariablesMetaData.class);

  private Map<TupleExpr, Set<Var>> variableMap;
  private VariableTypeMetaData variableTypeMetaData;

  /**
   * Constructs {@link VariablesMetaData}
   *
   * @param root root of query parse tree
   * @param variableTypeMetaData {@link VariableTypeMetaData} for the query
   */
  public VariablesMetaData(TupleExpr root, VariableTypeMetaData variableTypeMetaData) {
    super();
    this.variableMap = new HashMap<TupleExpr, Set<Var>>();
    this.variableTypeMetaData = variableTypeMetaData;
    process(root);
    // System.out.println(variableMap);
  }

  /**
   * returns the variables present in given {@link TupleExpr}
   *
   * @param tupleExpr expression
   * @return {@link Set} of variables in given expression
   */
  public Set<Var> extractVariables(TupleExpr tupleExpr) {
    return variableMap.get(tupleExpr);
  }

  /**
   *
   * Populates variableMap with {@link Set} of variables for given {@link StatementPattern}
   * expression
   *
   * @param tupleExpr
   */
  private void process(TupleExpr tupleExpr) {
    if (tupleExpr instanceof Filter) {
      process((Filter) tupleExpr);
    } else if (tupleExpr instanceof Extension) {
      process((Extension) tupleExpr);
    } else if (tupleExpr instanceof UnaryTupleOperator) {
      process((UnaryTupleOperator) tupleExpr);
    } else if (tupleExpr instanceof BinaryTupleOperator) {
      process((BinaryTupleOperator) tupleExpr);
    } else if (tupleExpr instanceof StatementPattern) {
      process((StatementPattern) tupleExpr);
    } else if (tupleExpr instanceof ArbitraryLengthPath) {
      process((ArbitraryLengthPath) tupleExpr);
    } else if (tupleExpr instanceof ZeroLengthPath) {
      process((ZeroLengthPath) tupleExpr);
    } else if (tupleExpr instanceof BindingSetAssignment) {
      process((BindingSetAssignment) tupleExpr);
    } else {
      logger.warn("Unsupported expression: " + tupleExpr);
      variableMap.put(tupleExpr, Collections.EMPTY_SET);
    }
  }

  /**
   *
   * Populates variableMap with {@link Set} of variables for given {@link ValueExpr} expression
   *
   * @param valueExpr
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
   *
   * Populates variableMap with {@link Set} of variables for given {@link BinaryValueOperator}
   * expression
   *
   * @param valueExpr
   */
  private void process(BinaryValueOperator valueExpr) {
    process(valueExpr.getLeftArg());
    process(valueExpr.getRightArg());
  }

  /**
   *
   * Populates variableMap with {@link Set} of variables for given {@link UnaryValueOperator}
   * expression
   *
   * @param valueExpr
   */
  private void process(UnaryValueOperator valueExpr) {
    process(valueExpr.getArg());
  }

  /**
   *
   * Populates variableMap with {@link Set} of variables for given {@link SubQueryValueOperator}
   * expression
   *
   * @param valueExpr
   */
  private void process(SubQueryValueOperator valueExpr) {
    process(valueExpr.getSubQuery());
  }

  /**
   *
   * Populates variableMap with {@link Set} of variables for given {@link StatementPattern}
   * expression
   *
   * @param tupleExpr
   */
  private void process(Extension tupleExpr) {
    process(tupleExpr.getArg());
    Set<Var> vars = new HashSet<Var>();
    for (ExtensionElem elem : tupleExpr.getElements()) {
      String asVar = elem.getName();
      vars.add(new Var(asVar));
    }
    vars.addAll(variableMap.get(tupleExpr.getArg()));
    variableMap.put(tupleExpr, vars);
  }

  /**
   *
   * Populates variableMap with {@link Set} of variables for given {@link StatementPattern}
   * expression
   *
   * @param tupleExpr
   */
  private void process(Filter tupleExpr) {
    process(tupleExpr.getCondition());
    process(tupleExpr.getArg());
    variableMap.put(tupleExpr, new HashSet<Var>(variableMap.get(tupleExpr.getArg())));
  }

  /**
   *
   * Populates variableMap with {@link Set} of variables for given {@link StatementPattern}
   * expression
   *
   * @param tupleExpr
   */
  private void process(UnaryTupleOperator tupleExpr) {
    process(tupleExpr.getArg());
    variableMap.put(tupleExpr, new HashSet<Var>(variableMap.get(tupleExpr.getArg())));
  }

  /**
   *
   * Populates variableMap with {@link Set} of variables for given {@link StatementPattern}
   * expression
   *
   * @param tupleExpr
   */
  private void process(BinaryTupleOperator tupleExpr) {
    process(tupleExpr.getLeftArg());
    process(tupleExpr.getRightArg());
    variableMap.put(tupleExpr, Sets.union(variableMap.get(tupleExpr.getLeftArg()),
        variableMap.get(tupleExpr.getRightArg())));
  }

  /**
   *
   * Populates variableMap with {@link Set} of variables for given {@link StatementPattern}
   * expression
   *
   * @param tupleExpr
   */
  private void process(StatementPattern tupleExpr) {
    Var subject = tupleExpr.getSubjectVar();
    Var object = tupleExpr.getObjectVar();
    Var predicate = tupleExpr.getPredicateVar();
    Set<Var> variables = new HashSet<Var>();
    if (isVariable(subject)) {
      variables.add(subject);
    }
    if (isVariable(predicate)) {
      variables.add(predicate);
    }
    if (isVariable(object)) {
      variables.add(object);
    }
    variableMap.put(tupleExpr, variables);
  }

  /**
   *
   * Populates variableMap with {@link Set} of variables for given {@link ArbitraryLengthPath}
   * expression
   *
   * @param tupleExpr
   */
  private void process(ArbitraryLengthPath tupleExpr) {
    process(tupleExpr.getPathExpression());
    variableMap.put(tupleExpr, new HashSet<Var>(variableMap.get(tupleExpr.getPathExpression())));
  }

  /**
   *
   * Populates variableMap with {@link Set} of variables for given {@link ZeroLengthPath} expression
   *
   * @param tupleExpr
   */
  private void process(ZeroLengthPath tupleExpr) {
    Var subject = tupleExpr.getSubjectVar();
    Var object = tupleExpr.getObjectVar();
    Set<Var> variables = new HashSet<Var>();
    if (isVariable(subject)) {
      variables.add(subject);
    }
    if (isVariable(object)) {
      variables.add(object);
    }
    variableMap.put(tupleExpr, variables);
  }

  /**
   *
   * Populates variableMap with {@link Set} of variables for given {@link BindingSetAssignment}
   * expression
   *
   * @param tupleExpr
   */
  private void process(BindingSetAssignment tupleExpr) {
    Set<Var> vars = new HashSet<Var>();
    for (String name : tupleExpr.getBindingNames()) {
      vars.add(new Var(name));
    }
    variableMap.put(tupleExpr, vars);
  }

  /**
   * Checks whether var is a variable or not
   *
   * @param var {@link Var} present in query
   * @return True if var is a variable false otherwise
   */
  private boolean isVariable(Var var) {
    return RDF4JHelper.isVariable(var);
    // && variableTypeMetaData.getVariableType(var.getName()) ==
    // VariableType.NODE_IRI;
  }

}
