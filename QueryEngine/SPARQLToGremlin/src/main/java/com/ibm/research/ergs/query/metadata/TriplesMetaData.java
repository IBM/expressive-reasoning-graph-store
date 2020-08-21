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
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.UnaryTupleOperator;
import org.eclipse.rdf4j.query.algebra.Var;
import com.ibm.research.ergs.query.utils.RDF4JHelper;

/**
 * This class is responsible for extracting StatementPattern containing a given variable
 *
 * @author Udit Sharma
 *
 */
public class TriplesMetaData {
  private Map<Var, StatementPattern> triples;
  PropertyTypeMetaData propertyTypeMetadata;

  /**
   * Constructs {@link TriplesMetaData}
   *
   * @param root root of the query parse tree
   * @param propertyTypeMetadata {@link PropertyTypeMetaData} for query
   */
  public TriplesMetaData(TupleExpr root, PropertyTypeMetaData propertyTypeMetadata) {
    super();
    this.triples = new HashMap<Var, StatementPattern>();
    this.propertyTypeMetadata = propertyTypeMetadata;
    process(root);
    // System.out.println(triples);
  }

  /**
   * Get triple having Variable var
   *
   * @param var variable
   * @return {@link StatementPattern}
   */
  public StatementPattern getTriples(Var var) {
    return triples.get(var);
  }

  /**
   * Populates triples with {@link StatementPattern} for variables in {@link TupleExpr}
   *
   * @param tupleExpr
   */
  private void process(TupleExpr tupleExpr) {
    if (tupleExpr instanceof UnaryTupleOperator) {
      process((UnaryTupleOperator) tupleExpr);
    } else if (tupleExpr instanceof BinaryTupleOperator) {
      process((BinaryTupleOperator) tupleExpr);
    } else if (tupleExpr instanceof StatementPattern) {
      process((StatementPattern) tupleExpr);
    } else if (tupleExpr instanceof ArbitraryLengthPath) {
      process((ArbitraryLengthPath) tupleExpr);
    } else {
      // Ignore
    }
  }

  /**
   * Populates triples with {@link StatementPattern} for variables in {@link UnaryTupleOperator}
   *
   * @param tupleExpr
   */
  private void process(UnaryTupleOperator tupleExpr) {
    process(tupleExpr.getArg());
  }

  /**
   * Populates triples with {@link StatementPattern} for variables in {@link BinaryTupleOperator}
   *
   * @param tupleExpr
   */
  private void process(BinaryTupleOperator tupleExpr) {
    process(tupleExpr.getLeftArg());
    process(tupleExpr.getRightArg());
  }

  /**
   * Populates triples with {@link StatementPattern} for variables in {@link StatementPattern}
   *
   * @param tupleExpr
   */
  private void process(StatementPattern tupleExpr) {
    // only populate for object and predicate
    Var object = tupleExpr.getObjectVar();
    Var predicate = tupleExpr.getPredicateVar();
    if (RDF4JHelper.isVariable(predicate)) {
      triples.put(predicate, tupleExpr);
    }
    if (RDF4JHelper.isVariable(object)) {
      triples.put(object, tupleExpr);
    }
  }

  /**
   * Populates triples with {@link StatementPattern} for variables in {@link ArbitraryLengthPath}
   *
   * @param tupleExpr
   */
  private void process(ArbitraryLengthPath tupleExpr) {
    process(tupleExpr.getPathExpression());
  }

}
