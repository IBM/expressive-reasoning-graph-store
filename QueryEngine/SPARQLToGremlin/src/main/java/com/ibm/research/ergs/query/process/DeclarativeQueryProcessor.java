/*******************************************************************************
 * Copyright 2020 IBM Corporation and others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *******************************************************************************/
package com.ibm.research.ergs.query.process;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.codehaus.groovy.ast.expr.TupleExpression;
import org.eclipse.rdf4j.query.algebra.ArbitraryLengthPath;
import org.eclipse.rdf4j.query.algebra.Difference;
import org.eclipse.rdf4j.query.algebra.Extension;
import org.eclipse.rdf4j.query.algebra.Filter;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.LeftJoin;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.UnaryTupleOperator;
import org.eclipse.rdf4j.query.algebra.Union;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.ZeroLengthPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.ibm.research.ergs.query.metadata.MetaData;
import com.ibm.research.ergs.query.utils.RDF4JHelper;

/**
 * It reorders the triple patterns in conjunctive SPARQL queries to obtain best possible query plan
 *
 * @author Udit Sharma
 *
 */
public class DeclarativeQueryProcessor {
  private static final Logger logger = LoggerFactory.getLogger(DeclarativeQueryProcessor.class);
  MetaData metaData;

  /**
   * Constructs {@link DeclarativeQueryProcessor}
   *
   * @param metaData metadata information for a query
   */
  public DeclarativeQueryProcessor(MetaData metaData) {
    super();
    this.metaData = metaData;
  }

  /**
   * Extracts and reorders triple patterns from {@link Join} expression
   *
   * @param join {@link Join} conjunctive query
   * @param cur {@link Set} of variables already visited by current traversal
   * @return ordered {@link List} of {@link TupleExpr} in {@link Join} expression
   */
  private List<TupleExpr> transform(Join join, Set<Var> cur) {
    List<TupleExpr> list = collectTupleExpr(join);
    transform(list, cur);
    return list;
  }

  /**
   * It reorders the input triple patterns to obtain best possible query plan
   *
   * @param list list of triple patterns in declartive conjunctive query
   * @param cur {@link Set} of variables already visited by current traversal
   * @return ordered list of triple patterns in optimal query plan
   */
  public List<TupleExpr> transform(List<TupleExpr> list, Set<Var> cur) {
    List<TupleExpr> ret = new ArrayList<TupleExpr>();
    while (!list.isEmpty()) {
      TupleExpr next = getNext(list, cur);
      cur.addAll(metaData.extractVariables(next));
      list.remove(next);
      ret.add(next);
    }
    return ret;
  }

  /**
   * Extracts triple patterns from {@link Join} expression
   *
   * @param tupleExpr join expression
   * @return list of triple patterns in input expression
   */
  private List<TupleExpr> collectTupleExpr(TupleExpr tupleExpr) {
    List<TupleExpr> list = new ArrayList<TupleExpr>();
    if (tupleExpr instanceof Join) {
      list.addAll(collectTupleExpr(((Join) tupleExpr).getLeftArg()));
      list.addAll(collectTupleExpr(((Join) tupleExpr).getRightArg()));
    } else {
      list.add(tupleExpr);
    }
    return list;
  }

  /**
   * Finds the next triple pattern to be traversed from the given list of expressions
   *
   * @param list list of triple patterns
   * @param cur {@link Set} of variables already visited by current traversal
   * @return next expression to be processed
   */
  private TupleExpr getNext(List<TupleExpr> list, Set<Var> cur) {
    TupleExpr ret = getConnected(list, cur);
    // If none of expression is reachable do cross product else do join
    if (ret == null) {
      ret = getFirst(list);
    }
    return ret;
  }

  /**
   * Finds the {@link TupleExpr} that can be traversed using already visited variables from the
   * given list of expressions
   *
   * @param list list of triple patterns
   * @param cur {@link Set} of variables already visited by current traversal
   * @return connected expression
   */
  private TupleExpr getConnected(List<TupleExpr> list, Set<Var> cur) {
    Map.Entry<TupleExpr, Long> connected = null;
    // find the most optimal tupleExpr
    for (TupleExpr tupleExpr : list) {
      connected = better(connected, getConnected(tupleExpr, cur));
    }
    if (connected != null) {
      return connected.getKey();
    } else {
      return null;
    }
  }

  /**
   * Finds the {@link TupleExpr} from the subtree of given tupleExpr that can be traversed using
   * already visited variables
   *
   * @param tupleExpr expression tree
   * @param cur {@link Set} of variables already visited by current traversal
   * @return most optimal expression reachable using already visited variables along with its cost
   */
  private Map.Entry<TupleExpr, Long> getConnected(TupleExpr tupleExpr, Set<Var> cur) {
    if (tupleExpr instanceof UnaryTupleOperator) {
      return getConnected((UnaryTupleOperator) tupleExpr, cur);
    } else if (tupleExpr instanceof Union) {
      return getConnected((Union) tupleExpr, cur);
    } else if (tupleExpr instanceof Join) {
      return getConnected((Join) tupleExpr, cur);
    } else if (tupleExpr instanceof LeftJoin) {
      return getConnected((LeftJoin) tupleExpr, cur);
    } else if (tupleExpr instanceof Difference) {
      return getConnected((Difference) tupleExpr, cur);
    } else if (tupleExpr instanceof StatementPattern) {
      return getConnected((StatementPattern) tupleExpr, cur);
    } else if (tupleExpr instanceof ZeroLengthPath) {
      return getConnected((ZeroLengthPath) tupleExpr, cur);
    } else if (tupleExpr instanceof ArbitraryLengthPath) {
      return getConnected((ArbitraryLengthPath) tupleExpr, cur);
    } else {
      // else ignore
      logger.warn("Unsupported expression: " + tupleExpr);
      return null;
    }
  }

  /**
   * Finds the {@link TupleExpr} from the subtree of given {@link UnaryTupleOperator} tupleExpr that
   * can be traversed using already visited variables
   *
   * @param tupleExpr expression tree
   * @param cur {@link Set} of variables already visited by current traversal
   * @return most optimal expression reachable using already visited variables along with its cost
   */
  private Map.Entry<TupleExpr, Long> getConnected(UnaryTupleOperator tupleExpr, Set<Var> cur) {
    return getConnected(tupleExpr.getArg(), cur);
  }

  /**
   * Finds the {@link TupleExpr} from the subtree of given {@link Join} tupleExpr that can be
   * traversed using already visited variables
   *
   * @param tupleExpr expression tree
   * @param cur {@link Set} of variables already visited by current traversal
   * @return most optimal expression reachable using already visited variables along with its cost
   */
  private Map.Entry<TupleExpr, Long> getConnected(Join tupleExpr, Set<Var> cur) {
    Map.Entry<TupleExpr, Long> left = getConnected(tupleExpr.getLeftArg(), cur);
    Map.Entry<TupleExpr, Long> right = getConnected(tupleExpr.getRightArg(), cur);
    // if both left and right child are not reachable return null else return better
    // of two
    if (left == null && right == null) {
      return null;
    } else {
      return new AbstractMap.SimpleEntry<TupleExpr, Long>(tupleExpr,
          better(left, right).getValue());
    }
  }

  /**
   * Finds the {@link TupleExpr} from the subtree of given {@link Union} tupleExpr that can be
   * traversed using already visited variables
   *
   * @param tupleExpr expression tree
   * @param cur {@link Set} of variables already visited by current traversal
   * @return most optimal expression reachable using already visited variables along with its cost
   */
  private Map.Entry<TupleExpr, Long> getConnected(Union tupleExpr, Set<Var> cur) {
    Map.Entry<TupleExpr, Long> left = getConnected(tupleExpr.getLeftArg(), cur);
    Map.Entry<TupleExpr, Long> right = getConnected(tupleExpr.getRightArg(), cur);
    // if either left or right child is not reachable return null else return
    // current tupleExpr
    if (left == null || right == null) {
      return null;
    } else {
      return new AbstractMap.SimpleEntry<TupleExpr, Long>(tupleExpr,
          left.getValue() + right.getValue());
    }
  }

  /**
   * Finds the {@link TupleExpr} from the subtree of given {@link Union} tupleExpr that can be
   * traversed using already visited variables
   *
   * @param tupleExpr expression tree
   * @param cur {@link Set} of variables already visited by current traversal
   * @return most optimal expression reachable using already visited variables along with its cost
   */
  private Map.Entry<TupleExpr, Long> getConnected(LeftJoin tupleExpr, Set<Var> cur) {
    Map.Entry<TupleExpr, Long> left = getConnected(tupleExpr.getLeftArg(), cur);
    // Map.Entry<TupleExpr, Long> right = getConnected(tupleExpr.getRightArg(),
    // cur);
    if (left == null) {
      return null;
    } else {
      return new AbstractMap.SimpleEntry<TupleExpr, Long>(tupleExpr, left.getValue());
    }
  }

  /**
   * Finds the {@link TupleExpr} from the subtree of given {@link Union} tupleExpr that can be
   * traversed using already visited variables
   *
   * @param tupleExpr expression tree
   * @param cur {@link Set} of variables already visited by current traversal
   * @return most optimal expression reachable using already visited variables along with its cost
   */
  private Map.Entry<TupleExpr, Long> getConnected(Difference tupleExpr, Set<Var> cur) {
    Map.Entry<TupleExpr, Long> left = getConnected(tupleExpr.getLeftArg(), cur);
    // Map.Entry<TupleExpr, Long> right = getConnected(tupleExpr.getRightArg(),
    // cur);
    if (left == null) {
      return null;
    } else {
      return new AbstractMap.SimpleEntry<TupleExpr, Long>(tupleExpr, left.getValue());
    }
  }

  /**
   * Finds the {@link TupleExpr} from the subtree of given {@link StatementPattern} tupleExpr that
   * can be traversed using already visited variables
   *
   * @param tupleExpr expression tree
   * @param cur {@link Set} of variables already visited by current traversal
   * @return most optimal expression reachable using already visited variables along with its cost
   */
  private Map.Entry<TupleExpr, Long> getConnected(StatementPattern tupleExpr, Set<Var> cur) {
    Var predicate = tupleExpr.getPredicateVar();
    String edgeLabel =
        String.valueOf((RDF4JHelper.isNull(predicate) || RDF4JHelper.isVariable(predicate)) ? null
            : RDF4JHelper.getValue(predicate.getValue()));
    // if subject is reachable
    if (cur.contains(tupleExpr.getSubjectVar())) {
      return new AbstractMap.SimpleEntry<TupleExpr, Long>(tupleExpr,
          metaData.getRelationshipRangeCardinalityForFixedDomain(edgeLabel));
    }
    // if object is reachable
    else if (cur.contains(tupleExpr.getObjectVar())) {
      return new AbstractMap.SimpleEntry<TupleExpr, Long>(tupleExpr,
          metaData.getRelationshipDomainCardinalityForFixedRange(edgeLabel));
    } else {
      return null;
    }
  }

  /**
   * Finds the {@link TupleExpr} from the subtree of given {@link ZeroLengthPath} tupleExpr that can
   * be traversed using already visited variables
   *
   * @param tupleExpr expression tree
   * @param cur {@link Set} of variables already visited by current traversal
   * @return most optimal expression reachable using already visited variables along with its cost
   */
  private Map.Entry<TupleExpr, Long> getConnected(ZeroLengthPath tupleExpr, Set<Var> cur) {
    if (cur.contains(tupleExpr.getSubjectVar())) {
      return new AbstractMap.SimpleEntry<TupleExpr, Long>(tupleExpr, 1l);
    } else if (cur.contains(tupleExpr.getObjectVar())) {
      return new AbstractMap.SimpleEntry<TupleExpr, Long>(tupleExpr, 1l);
    } else {
      return null;
    }
  }

  /**
   * Finds the {@link TupleExpr} from the subtree of given {@link ArbitraryLengthPath} tupleExpr
   * that can be traversed using already visited variables
   *
   * @param tupleExpr expression tree
   * @param cur {@link Set} of variables already visited by current traversal
   * @return most optimal expression reachable using already visited variables along with its cost
   */
  private Map.Entry<TupleExpr, Long> getConnected(ArbitraryLengthPath tupleExpr, Set<Var> cur) {
    Map.Entry<TupleExpr, Long> ret = getConnected(tupleExpr.getPathExpression(), cur);
    if (ret == null) {
      return ret;
    } else {
      return new AbstractMap.SimpleEntry<TupleExpr, Long>(tupleExpr, ret.getValue());
    }
  }


  /**
   * Finds the most optimal {@link TupleExpr} that can be used to start a new traversal from the
   * given list of expressions
   *
   * @param list list of triple patterns
   * @return {@link TupleExpression} that can be used to start a new traversal
   */
  private TupleExpr getFirst(List<TupleExpr> list) {
    Map.Entry<TupleExpr, Long> ret = getFirst(list.get(0));
    for (TupleExpr tupleExpr : list) {
      ret = better(ret, getFirst(tupleExpr));
    }
    return ret.getKey();
  }

  /**
   * Finds the most optimal {@link TupleExpr} from the subtree of given {@link TupleExpr} tupleExpr
   * that can be used to start a new traversal
   *
   * @param tupleExpr expression tree
   * @return {@link TupleExpression} that can be used to start a new traversal along with its cost
   */
  private Map.Entry<TupleExpr, Long> getFirst(TupleExpr tupleExpr) {
    if (tupleExpr instanceof Filter) {
      return getFirst((Filter) tupleExpr);
    } else if (tupleExpr instanceof Extension) {
      return getFirst((Extension) tupleExpr);
    } else if (tupleExpr instanceof UnaryTupleOperator) {
      return getFirst((UnaryTupleOperator) tupleExpr);
    } else if (tupleExpr instanceof Union) {
      return getFirst((Union) tupleExpr);
    } else if (tupleExpr instanceof Join) {
      return getFirst((Join) tupleExpr);
    } else if (tupleExpr instanceof LeftJoin) {
      return getFirst((LeftJoin) tupleExpr);
    } else if (tupleExpr instanceof Difference) {
      return getFirst((Difference) tupleExpr);
    } else if (tupleExpr instanceof StatementPattern) {
      return getFirst((StatementPattern) tupleExpr);
    } else if (tupleExpr instanceof ZeroLengthPath) {
      return getFirst((ZeroLengthPath) tupleExpr);
    } else if (tupleExpr instanceof ArbitraryLengthPath) {
      return getFirst((ArbitraryLengthPath) tupleExpr);
    } else {
      // else ignore
      logger.warn("Unsupported expression: " + tupleExpr);
      return null;
    }
  }

  /**
   * Finds the most optimal {@link TupleExpr} from the subtree of given {@link Filter} tupleExpr
   * that can be used to start a new traversal
   *
   * @param tupleExpr expression tree
   * @return {@link TupleExpression} that can be used to start a new traversal along with its cost
   */
  private Map.Entry<TupleExpr, Long> getFirst(Filter tupleExpr) {
    Long filterCost = metaData.getIndexedFilter(tupleExpr).stream().map(a -> a.getRank()).reduce(0L,
        (a, b) -> a + b);
    return new AbstractMap.SimpleEntry<TupleExpr, Long>(tupleExpr, filterCost);
  }

  /**
   * Finds the most optimal {@link TupleExpr} from the subtree of given {@link Extension} tupleExpr
   * that can be used to start a new traversal
   *
   * @param tupleExpr expression tree
   * @return {@link TupleExpression} that can be used to start a new traversal along with its cost
   */
  private Map.Entry<TupleExpr, Long> getFirst(Extension tupleExpr) {
    Long filterCost = metaData.getIndexedFilter(tupleExpr).stream().map(a -> a.getRank()).reduce(0L,
        (a, b) -> a + b);
    return new AbstractMap.SimpleEntry<TupleExpr, Long>(tupleExpr, filterCost);
  }

  /**
   * Finds the most optimal {@link TupleExpr} from the subtree of given {@link UnaryTupleOperator}
   * tupleExpr that can be used to start a new traversal
   *
   * @param tupleExpr expression tree
   * @return {@link TupleExpression} that can be used to start a new traversal along with its cost
   */
  private Map.Entry<TupleExpr, Long> getFirst(UnaryTupleOperator tupleExpr) {
    return getFirst(tupleExpr.getArg());
  }

  /**
   * Finds the most optimal {@link TupleExpr} from the subtree of given {@link Union} tupleExpr that
   * can be used to start a new traversal
   *
   * @param tupleExpr expression tree
   * @return {@link TupleExpression} that can be used to start a new traversal along with its cost
   */
  private Map.Entry<TupleExpr, Long> getFirst(Union tupleExpr) {
    Map.Entry<TupleExpr, Long> left = getFirst(tupleExpr.getLeftArg());
    Map.Entry<TupleExpr, Long> right = getFirst(tupleExpr.getRightArg());
    if (left == null || right == null) {
      return null;
    } else {
      return new AbstractMap.SimpleEntry<TupleExpr, Long>(tupleExpr,
          left.getValue() + right.getValue());
    }
  }

  /**
   * Finds the most optimal {@link TupleExpr} from the subtree of given {@link Join} tupleExpr that
   * can be used to start a new traversal
   *
   * @param tupleExpr expression tree
   * @return {@link TupleExpression} that can be used to start a new traversal along with its cost
   */
  private Map.Entry<TupleExpr, Long> getFirst(Join tupleExpr) {
    Map.Entry<TupleExpr, Long> left = getFirst(tupleExpr.getLeftArg());
    Map.Entry<TupleExpr, Long> right = getFirst(tupleExpr.getRightArg());
    if (left == null && right == null) {
      return null;
    } else {
      return new AbstractMap.SimpleEntry<TupleExpr, Long>(tupleExpr,
          better(left, right).getValue());
    }
  }

  /**
   * Finds the most optimal {@link TupleExpr} from the subtree of given {@link LeftJoin} tupleExpr
   * that can be used to start a new traversal
   *
   * @param tupleExpr expression tree
   * @return {@link TupleExpression} that can be used to start a new traversal along with its cost
   */
  private Map.Entry<TupleExpr, Long> getFirst(LeftJoin tupleExpr) {
    Map.Entry<TupleExpr, Long> left = getFirst(tupleExpr.getLeftArg());
    // Map.Entry<TupleExpr, Long> right = getFirst(tupleExpr.getRightArg());
    if (left == null) {
      return null;
    } else {
      return new AbstractMap.SimpleEntry<TupleExpr, Long>(tupleExpr, left.getValue());
    }
  }

  /**
   * Finds the most optimal {@link TupleExpr} from the subtree of given {@link Difference} tupleExpr
   * that can be used to start a new traversal
   *
   * @param tupleExpr expression tree
   * @return {@link TupleExpression} that can be used to start a new traversal along with its cost
   */
  private Map.Entry<TupleExpr, Long> getFirst(Difference tupleExpr) {
    Map.Entry<TupleExpr, Long> left = getFirst(tupleExpr.getLeftArg());
    // Map.Entry<TupleExpr, Long> right = getFirst(tupleExpr.getRightArg());
    if (left == null) {
      return null;
    } else {
      return new AbstractMap.SimpleEntry<TupleExpr, Long>(tupleExpr, left.getValue());
    }
  }

  /**
   * Finds the most optimal {@link TupleExpr} from the subtree of given {@link StatementPattern}
   * tupleExpr that can be used to start a new traversal
   *
   * @param tupleExpr expression tree
   * @return {@link TupleExpression} that can be used to start a new traversal along with its cost
   */
  private Map.Entry<TupleExpr, Long> getFirst(StatementPattern tupleExpr) {
    Long filterCost = metaData.getIndexedFilter(tupleExpr).stream().map(a -> a.getRank()).reduce(0L,
        (a, b) -> a + b);
    return new AbstractMap.SimpleEntry<TupleExpr, Long>(tupleExpr, filterCost);
  }

  /**
   * Finds the most optimal {@link TupleExpr} from the subtree of given {@link ZeroLengthPath}
   * tupleExpr that can be used to start a new traversal
   *
   * @param tupleExpr expression tree
   * @return {@link TupleExpression} that can be used to start a new traversal along with its cost
   */
  private Map.Entry<TupleExpr, Long> getFirst(ZeroLengthPath tupleExpr) {
    Long filterCost = metaData.getIndexedFilter(tupleExpr).stream().map(a -> a.getRank()).reduce(0L,
        (a, b) -> a + b);
    return new AbstractMap.SimpleEntry<TupleExpr, Long>(tupleExpr, filterCost);
  }

  /**
   * Finds the most optimal {@link TupleExpr} from the subtree of given {@link ArbitraryLengthPath}
   * tupleExpr that can be used to start a new traversal
   *
   * @param tupleExpr expression tree
   * @return {@link TupleExpression} that can be used to start a new traversal along with its cost
   */
  private Map.Entry<TupleExpr, Long> getFirst(ArbitraryLengthPath tupleExpr) {
    Map.Entry<TupleExpr, Long> ret = getFirst(tupleExpr.getPathExpression());
    if (ret == null) {
      return ret;
    } else {
      return new AbstractMap.SimpleEntry<TupleExpr, Long>(tupleExpr, ret.getValue());
    }

  }

  private Map.Entry<TupleExpr, Long> better(Map.Entry<TupleExpr, Long> first,
      Map.Entry<TupleExpr, Long> second) {
    if (first == null) {
      return second;
    }
    if (second == null) {
      return first;
    }
    long rankFirst = first.getValue();
    long rankSecond = second.getValue();

    return rankFirst < rankSecond ? first : second;
  }
}
