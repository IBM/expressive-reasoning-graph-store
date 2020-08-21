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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.eclipse.rdf4j.query.algebra.ArbitraryLengthPath;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.Union;
import org.eclipse.rdf4j.query.algebra.Var;
import com.ibm.research.ergs.query.utils.RDF4JHelper;

/**
 * This class is responsible to translating Arbitrary length property path expressions in SPARQL
 * query to equivalent Gremlin traversals
 *
 * @author Udit Sharma
 *
 */
public class TransformArbitraryLengthTriple {

  /**
   * It converts arbitrary length property path expression
   *
   * @param traversal current traversal
   * @param triple triple pattern
   * @param isInverted True if traversal should start from object, False if it should start from
   *        object of triple
   * @param queryData data specific to given query
   */
  public static void transform(GraphTraversal<?, ?> traversal, ArbitraryLengthPath path,
      boolean isInverted, QueryData queryData) {
    Var subject = path.getSubjectVar();
    Var object = path.getObjectVar();
    List<Var> predicate = getPredicateVars(path.getPathExpression());
    transform(traversal, subject, predicate, object, path.getMinLength(), isInverted, queryData);
    // transform(traversal, path.getPathExpression(), path.getMinLength(),
    // isInverted, metaData);
  }

  /**
   * It collects all the predicates present in union of multiple Statement Patterns
   *
   * @param tupleExpr expression
   * @return {@link List} of predicates
   */
  private static List<Var> getPredicateVars(TupleExpr tupleExpr) {
    if (tupleExpr instanceof StatementPattern) {
      return Collections.singletonList(((StatementPattern) tupleExpr).getPredicateVar());
    } else {
      List<Var> ret = new ArrayList<Var>();
      ret.addAll(getPredicateVars(((Union) tupleExpr).getLeftArg()));
      ret.addAll(getPredicateVars(((Union) tupleExpr).getRightArg()));
      return ret;
    }
  }

  /**
   * It converts property path expressions to equivalent Gremlin traversal
   *
   * @param traversal current traversal
   * @param subject subject of the path expression
   * @param predicate {@link List} of predicates in the path expression
   * @param object object of the path expression
   * @param minLength minimum length of path expressions
   * @param isInverted True if traversal should start from object, False if it should start from
   *        object of path expression
   * @param queryData data specific to given query
   */
  private static void transform(GraphTraversal<?, ?> traversal, Var subject, List<Var> predicate,
      Var object, long minLength, boolean isInverted, QueryData queryData) {
    boolean isZeroLengthPath = (minLength == 0);

    // Case 1
    if (RDF4JHelper.isVariable(subject) && RDF4JHelper.isVariable(object)) {
      ArrayList<GraphTraversal<?, ?>> traversal1 = null;
      switch (queryData.getQueryMetaData().getVariableType(object.getName())) {
        case LITERAL:
          getDataProperties(traversal, subject, predicate, object, queryData);
          break;
        case NODE_IRI:
          getObjectProperties(traversal, subject, predicate, object, isInverted, isZeroLengthPath,
              queryData);
          break;
      }
    }

    // Case 2
    else if (RDF4JHelper.isVariable(subject) && RDF4JHelper.isConcrete(object)) {
      if (RDF4JHelper.isLiteral(object)) {
        getDataProperties(traversal, subject, predicate, object, queryData);
      } else {
        getObjectProperties(traversal, subject, predicate, object, isInverted, isZeroLengthPath,
            queryData);
      }

    }

    // Case 3
    else if (RDF4JHelper.isIRI(subject) && RDF4JHelper.isVariable(object)) {
      ArrayList<GraphTraversal<?, ?>> traversal1 = null;
      switch (queryData.getQueryMetaData().getVariableType(object.getName())) {
        case LITERAL:
          getDataProperties(traversal, subject, predicate, object, queryData);
          break;
        case NODE_IRI:
          getObjectProperties(traversal, subject, predicate, object, isInverted, isZeroLengthPath,
              queryData);
          break;

      }
    }

  }

  /**
   * It converts property path expressions to equivalent Gremlin traversal when predicate is object
   * property
   *
   * @param traversal current traversal
   * @param subject subject of the path expression
   * @param predicate {@link List} of predicates in the path expression
   * @param object object of the path expression
   * @param isInverted True if traversal should start from object, False if it should start from
   *        object of path expression
   * @param isZeroLengthPath True if minimum Lengh is zero, False otherwise
   * @param queryData data specific to given query
   */
  private static void getObjectProperties(GraphTraversal<?, ?> traversal, Var subject,
      List<Var> predicate, Var object, boolean isInverted, boolean isZeroLengthPath,
      QueryData queryData) {
    if (isInverted) {
      getReverseObjectProperties(traversal, subject, predicate, object, isZeroLengthPath,
          queryData);
    } else {
      getObjectProperties(traversal, subject, predicate, object, isZeroLengthPath, queryData);
    }
  }

  /**
   * It converts property path expressions to equivalent Gremlin traversal when predicate is object
   * property and direction of traversal is from subject
   *
   * @param traversal current traversal
   * @param subject subject of the path expression
   * @param predicate {@link List} of predicates in the path expression
   * @param object object of the path expression
   * @param isZeroLengthPath True if minimum Lengh is zero, False otherwise
   * @param queryData data specific to given query
   */
  private static void getObjectProperties(GraphTraversal<?, ?> traversal, Var subject,
      List<Var> predicate, Var object, boolean isZeroLengthPath, QueryData queryData) {
    if (isZeroLengthPath) {
      getZeroOrMoreLengthObjectProperties(traversal, subject, predicate, object, queryData);
    } else {
      getOneOrMoreLengthObjectProperties(traversal, subject, predicate, object, queryData);
    }
  }

  /**
   * It converts property path expressions to equivalent Gremlin traversal when predicate is object
   * property and direction of traversal is from object
   *
   * @param traversal current traversal
   * @param subject subject of the path expression
   * @param predicate {@link List} of predicates in the path expression
   * @param object object of the path expression
   * @param isZeroLengthPath True if minimum Lengh is zero, False otherwise
   * @param queryData data specific to given query
   */
  private static void getReverseObjectProperties(GraphTraversal<?, ?> traversal, Var subject,
      List<Var> predicate, Var object, boolean isZeroLengthPath, QueryData queryData) {
    if (isZeroLengthPath) {
      getZeroOrMoreLengthReverseObjectProperties(traversal, subject, predicate, object, queryData);
    } else {
      getOneOrMoreLengthReverseObjectProperties(traversal, subject, predicate, object, queryData);
    }
  }

  /**
   * It converts property path expressions to equivalent Gremlin traversal when predicate is object
   * property, direction of traversal is from subject and minimum path length is zero
   *
   * @param traversal current traversal
   * @param subject subject of the path expression
   * @param predicate {@link List} of predicates in the path expression
   * @param object object of the path expression
   * @param queryData data specific to given query
   */
  private static void getZeroOrMoreLengthObjectProperties(GraphTraversal<?, ?> traversal,
      Var subject, List<Var> predicate, Var object, QueryData queryData) {
    String subjVar = queryData.getAnonymousVariable(subject);
    if (RDF4JHelper.isIRI(subject)) {
      traversal = traversal.select(subjVar).has("id", RDF4JHelper.getValue(subject.getValue()));
    } else if (RDF4JHelper.isVariable(subject)) {
      traversal = traversal.select(subjVar).values("id").as(subject.getName());
    }

    String[] predicates = predicate.stream().map(pred -> RDF4JHelper.getPredicatValue(pred, false))
        .collect(Collectors.toList()).toArray(new String[0]);
    String objVar = queryData.getAnonymousVariable(object);
    traversal = traversal.select(subjVar).emit().repeat((Traversal) __.out(predicates)).as(objVar);
    if (RDF4JHelper.isIRI(object)) {
      traversal = traversal.select(objVar).values("id").is(RDF4JHelper.getValue(object.getValue()));
    } else if (RDF4JHelper.isVariable(object)) {
      traversal = traversal.select(objVar).values("id").as(object.getName());
    }
  }

  /**
   * It converts property path expressions to equivalent Gremlin traversal when predicate is object
   * property, direction of traversal is from subject and minimum path length is one
   *
   * @param traversal current traversal
   * @param subject subject of the path expression
   * @param predicate {@link List} of predicates in the path expression
   * @param object object of the path expression
   * @param queryData data specific to given query
   */
  private static void getOneOrMoreLengthObjectProperties(GraphTraversal<?, ?> traversal,
      Var subject, List<Var> predicate, Var object, QueryData queryData) {
    String subjVar = queryData.getAnonymousVariable(subject);
    if (RDF4JHelper.isIRI(subject)) {
      traversal = traversal.select(subjVar).has("id", RDF4JHelper.getValue(subject.getValue()));
    } else if (RDF4JHelper.isVariable(subject)) {
      traversal = traversal.select(subjVar).values("id").as(subject.getName());
    }

    String[] predicates = predicate.stream().map(pred -> RDF4JHelper.getPredicatValue(pred, false))
        .collect(Collectors.toList()).toArray(new String[0]);
    String objVar = queryData.getAnonymousVariable(object);
    traversal = traversal.select(subjVar).repeat((Traversal) __.out(predicates)).emit().as(objVar);

    if (RDF4JHelper.isIRI(object)) {
      traversal = traversal.select(objVar).values("id").is(RDF4JHelper.getValue(object.getValue()));
    } else if (RDF4JHelper.isVariable(object)) {
      traversal = traversal.select(objVar).values("id").as(object.getName());
    }
  }

  /**
   * It converts property path expressions to equivalent Gremlin traversal when predicate is object
   * property, direction of traversal is from object and minimum path length is zero
   *
   * @param traversal current traversal
   * @param subject subject of the path expression
   * @param predicate {@link List} of predicates in the path expression
   * @param object object of the path expression
   * @param queryData data specific to given query
   */
  private static void getZeroOrMoreLengthReverseObjectProperties(GraphTraversal<?, ?> traversal,
      Var subject, List<Var> predicate, Var object, QueryData queryData) {
    String objVar = queryData.getAnonymousVariable(object);
    if (RDF4JHelper.isIRI(object)) {
      traversal = traversal.select(objVar).has("id", RDF4JHelper.getValue(object.getValue()));
    } else if (RDF4JHelper.isVariable(object)) {
      traversal = traversal.select(objVar).values("id").as(object.getName());
    }

    String subjVar = queryData.getAnonymousVariable(subject);
    String[] predicates = predicate.stream().map(pred -> RDF4JHelper.getPredicatValue(pred, false))
        .collect(Collectors.toList()).toArray(new String[0]);
    traversal = traversal.select(objVar).emit().repeat((Traversal) __.in(predicates)).as(subjVar);
    if (RDF4JHelper.isIRI(subject)) {
      traversal =
          traversal.select(subjVar).values("id").is(RDF4JHelper.getValue(subject.getValue()));
    } else if (RDF4JHelper.isVariable(subject)) {
      traversal = traversal.select(subjVar).values("id").as(subject.getName());
    }
  }

  /**
   * It converts property path expressions to equivalent Gremlin traversal when predicate is object
   * property, direction of traversal is from object and minimum path length is one
   *
   * @param traversal current traversal
   * @param subject subject of the path expression
   * @param predicate {@link List} of predicates in the path expression
   * @param object object of the path expression
   * @param queryData data specific to given query
   */
  private static void getOneOrMoreLengthReverseObjectProperties(GraphTraversal<?, ?> traversal,
      Var subject, List<Var> predicate, Var object, QueryData queryData) {
    String objVar = queryData.getAnonymousVariable(object);
    if (RDF4JHelper.isIRI(object)) {
      traversal = traversal.select(objVar).has("id", RDF4JHelper.getValue(object.getValue()));
    } else if (RDF4JHelper.isVariable(object)) {
      traversal = traversal.select(objVar).values("id").as(object.getName());
    }

    String[] predicates = predicate.stream().map(pred -> RDF4JHelper.getPredicatValue(pred, false))
        .collect(Collectors.toList()).toArray(new String[0]);
    String subjVar = queryData.getAnonymousVariable(subject);
    traversal = traversal.select(objVar).repeat((Traversal) __.in(predicates)).emit().as(subjVar);
    if (RDF4JHelper.isIRI(subject)) {
      traversal =
          traversal.select(subjVar).values("id").is(RDF4JHelper.getValue(subject.getValue()));
    } else if (RDF4JHelper.isVariable(subject)) {
      traversal = traversal.select(subjVar).values("id").as(subject.getName());
    }
  }

  /**
   * It converts property path expressions to equivalent Gremlin traversal when predicate is data
   * type property
   *
   * @param traversal current traversal
   * @param subject subject of the path expression
   * @param predicate {@link List} of predicates in the path expression
   * @param object object of the path expression
   * @param queryData data specific to given query
   */
  private static void getDataProperties(GraphTraversal<?, ?> traversal, Var subject,
      List<Var> predicate, Var object, QueryData queryData) {
    String subjVar = queryData.getAnonymousVariable(subject);
    if (RDF4JHelper.isIRI(subject)) {
      traversal = traversal.select(subjVar).has("id", RDF4JHelper.getValue(subject.getValue()));
    } else if (RDF4JHelper.isVariable(subject)) {
      traversal = traversal.select(subjVar).values("id").as(subject.getName());
    }

    String[] predicates = predicate.stream().map(pred -> RDF4JHelper.getPredicatValue(pred, true))
        .collect(Collectors.toList()).toArray(new String[0]);
    String objVar = queryData.getAnonymousVariable(object);
    traversal = traversal.select(subjVar).values(predicates).unfold().as(objVar);

    if (RDF4JHelper.isLiteral(object)) {
      traversal = traversal.select(objVar).is(RDF4JHelper.getValue(object.getValue()));
    } else if (RDF4JHelper.isVariable(object)) {
      traversal = traversal.select(objVar).identity().as(object.getName());
    }
  }

}
