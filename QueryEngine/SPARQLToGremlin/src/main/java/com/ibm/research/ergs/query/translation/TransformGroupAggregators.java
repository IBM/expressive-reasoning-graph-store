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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.eclipse.rdf4j.query.algebra.AggregateOperator;
import org.eclipse.rdf4j.query.algebra.Avg;
import org.eclipse.rdf4j.query.algebra.Count;
import org.eclipse.rdf4j.query.algebra.GroupConcat;
import org.eclipse.rdf4j.query.algebra.Max;
import org.eclipse.rdf4j.query.algebra.Min;
import org.eclipse.rdf4j.query.algebra.Sample;
import org.eclipse.rdf4j.query.algebra.Sum;
import org.eclipse.rdf4j.query.algebra.ValueConstant;
import org.eclipse.rdf4j.query.algebra.Var;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is responsible to translating GROUP aggregate expressions in SPARQL query to
 * equivalent Gremlin traversals
 *
 * @author Udit Sharma
 *
 */
public class TransformGroupAggregators {
  private static final Logger logger = LoggerFactory.getLogger(TransformGroupAggregators.class);

  /**
   * This method uses reflection to invoke appropriate method depending upon the type of
   * AggregateOperator
   *
   * @param aggregateOperator aggregate expression
   * @param queryData data specific to given query
   */
  public static GraphTraversal<?, ?> transform(AggregateOperator aggregateOperator,
      QueryData queryData) {
    Class group = TransformGroupAggregators.class;
    try {
      Method methodcall =
          group.getDeclaredMethod("transform", aggregateOperator.getClass(), QueryData.class);
      GraphTraversal<?, ?> traversal =
          (GraphTraversal<?, ?>) methodcall.invoke(null, aggregateOperator, queryData);
      return traversal;
    } catch (NoSuchMethodException | SecurityException | IllegalAccessException
        | IllegalArgumentException | InvocationTargetException e) {
      logger.error(e.getMessage(), e);
      return null;
    }
  }

  /**
   * This method converts {@link Count} filter expression
   * {@see <a href= "https://www.w3.org/TR/sparql11-query/#defn_aggCount">COUNT</a>}
   *
   * @param aggregateOperator {@link Count} aggregate expression
   * @param queryData data specific to given query
   */
  private static GraphTraversal<?, ?> transform(Count aggregateOperator, QueryData queryData) {
    GraphTraversal<?, ?> ret = __.as("group").unfold();
    String var = "";
    Var expr = (Var) aggregateOperator.getArg();

    if (expr != null) {
      var = expr.getName();
      ret = ret.select(var);
    }
    if (aggregateOperator.isDistinct()) {
      ret = ret.dedup();
    }

    String asVar = aggregateOperator.getSignature() + "_" + var;
    queryData.addAlias(aggregateOperator, asVar);

    return ret.count().as(asVar);
  }

  /**
   * This method converts {@link Sum} filter expression
   * {@see <a href= "https://www.w3.org/TR/sparql11-query/#defn_aggSum">SUM</a>}
   *
   * @param aggregateOperator {@link Sum} aggregate expression
   * @param queryData data specific to given query
   */
  private static GraphTraversal<?, ?> transform(Sum aggregateOperator, QueryData queryData) {
    GraphTraversal<?, ?> ret = __.as("group").unfold();
    String var = "";
    Var expr = (Var) aggregateOperator.getArg();

    if (expr != null) {
      var = expr.getName();
      ret = ret.select(var);
    }
    if (aggregateOperator.isDistinct()) {
      ret = ret.dedup();
    }

    String asVar = aggregateOperator.getSignature() + "_" + var;
    queryData.addAlias(aggregateOperator, asVar);

    return ret.sum().as(asVar);
  }

  /**
   * This method converts {@link Min} filter expression
   * {@see <a href= "https://www.w3.org/TR/sparql11-query/#defn_aggMin">MIN</a>}
   *
   * @param aggregateOperator {@link Min} aggregate expression
   * @param queryData data specific to given query
   */
  private static GraphTraversal<?, ?> transform(Min aggregateOperator, QueryData queryData) {
    GraphTraversal<?, ?> ret = __.as("group").unfold();
    String var = "";
    Var expr = (Var) aggregateOperator.getArg();

    if (expr != null) {
      var = expr.getName();
      ret = ret.select(var);
    }
    if (aggregateOperator.isDistinct()) {
      ret = ret.dedup();
    }

    String asVar = aggregateOperator.getSignature() + "_" + var;
    queryData.addAlias(aggregateOperator, asVar);

    return ret.min().as(asVar);
  }

  /**
   * This method converts {@link Max} filter expression
   * {@see <a href= "https://www.w3.org/TR/sparql11-query/#defn_aggMax">MAX</a>}
   *
   * @param aggregateOperator {@link Max} aggregate expression
   * @param queryData data specific to given query
   */
  private static GraphTraversal<?, ?> transform(Max aggregateOperator, QueryData queryData) {
    GraphTraversal<?, ?> ret = __.as("group").unfold();
    String var = "";
    Var expr = (Var) aggregateOperator.getArg();

    if (expr != null) {
      var = expr.getName();
      ret = ret.select(var);
    }
    if (aggregateOperator.isDistinct()) {
      ret = ret.dedup();
    }

    String asVar = aggregateOperator.getSignature() + "_" + var;
    queryData.addAlias(aggregateOperator, asVar);

    return ret.max().as(asVar);
  }

  /**
   * This method converts {@link Avg} filter expression
   * {@see <a href= "https://www.w3.org/TR/sparql11-query/#defn_aggAvg">AVG</a>}
   *
   * @param aggregateOperator {@link Avg} aggregate expression
   * @param queryData data specific to given query
   */
  private static GraphTraversal<?, ?> transform(Avg aggregateOperator, QueryData queryData) {
    GraphTraversal<?, ?> ret = __.as("group").unfold();
    String var = "";
    Var expr = (Var) aggregateOperator.getArg();

    if (expr != null) {
      var = expr.getName();
      ret = ret.select(var);
    }
    if (aggregateOperator.isDistinct()) {
      ret = ret.dedup();
    }

    String asVar = aggregateOperator.getSignature() + "_" + var;
    queryData.addAlias(aggregateOperator, asVar);

    return ret.mean().as(asVar);
  }

  /**
   * This method converts {@link Sample} filter expression
   * {@see <a href= "https://www.w3.org/TR/sparql11-query/#defn_aggSample">SAMPLE</a>}
   *
   * @param aggregateOperator {@link Sample} aggregate expression
   * @param queryData data specific to given query
   */
  private static GraphTraversal<?, ?> transform(Sample aggregateOperator, QueryData queryData) {
    GraphTraversal<?, ?> ret = __.as("group").unfold();
    String var = "";
    Var expr = (Var) aggregateOperator.getArg();

    if (expr != null) {
      var = expr.getName();
      ret = ret.select(var);
    }
    if (aggregateOperator.isDistinct()) {
      ret = ret.dedup();
    }

    String asVar = aggregateOperator.getSignature() + "_" + var;
    queryData.addAlias(aggregateOperator, asVar);

    return ret.sample(1).as(asVar);
  }

  /**
   * This method converts {@link GroupConcat} filter expression
   * {@see <a href= "https://www.w3.org/TR/sparql11-query/#defn_aggGroupConcat">CONCAT</a>}
   *
   * @param aggregateOperator {@link GroupConcat} aggregate expression
   * @param queryData data specific to given query
   */
  private static GraphTraversal<?, ?> transform(GroupConcat aggregateOperator,
      QueryData queryData) {
    GraphTraversal<?, ?> ret = __.as("group").unfold();
    String var = "";
    Var expr = (Var) aggregateOperator.getArg();
    String separator =
        ((ValueConstant) (aggregateOperator.getSeparator())).getValue().stringValue();

    if (expr != null) {
      var = expr.getName();
      ret = ret.select(var);
    }
    if (aggregateOperator.isDistinct()) {
      ret = ret.dedup();
    }
    String asVar = aggregateOperator.getSignature() + "_" + var;
    queryData.addAlias(aggregateOperator, asVar);
    return ret.fold("", ((a, b) -> (a + (a.length() == 0 ? "" : separator) + b))).as(asVar);
  }
}
