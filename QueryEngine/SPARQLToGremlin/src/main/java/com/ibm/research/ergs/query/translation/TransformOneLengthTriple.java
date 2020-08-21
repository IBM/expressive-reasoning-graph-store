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

import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.TextP;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Var;
import com.ibm.research.ergs.query.metadata.VariableTypeMetaData.VariableType;
import com.ibm.research.ergs.query.utils.RDF4JHelper;

/**
 * This class is responsible to translating Triple Patterns in SPARQL query to equivalent Gremlin
 * traversals
 *
 * @author Udit Sharma
 *
 */
public class TransformOneLengthTriple {

  /**
   * It converts triple to equivalent Gremlin traversal
   *
   * @param traversal current traversal
   * @param triple triple pattern
   * @param isInverted True if traversal should start from object, False if it should start from
   *        object of triple
   * @param queryData data specific to given query
   */
  public static void transform(GraphTraversal<?, ?> traversal, StatementPattern triple,
      boolean isInverted, QueryData queryData) {
    Var subject = triple.getSubjectVar();
    Var predicate = triple.getPredicateVar();
    Var object = triple.getObjectVar();
    // Case 1
    if (RDF4JHelper.isVariable(subject) && RDF4JHelper.isVariable(predicate)
        && RDF4JHelper.isVariable(object)) {
      switch (queryData.getQueryMetaData().getVariableType(object.getName())) {
        case LITERAL:
          TransformOneLengthTriple.getDataProperties(traversal, subject, predicate, object,
              queryData);
          break;
        case NODE_IRI:
          TransformOneLengthTriple.getObjectProperties(traversal, subject, predicate, object,
              isInverted, queryData);
          break;
        case ANY:
          TransformOneLengthTriple.getDataAndObjectProperties(traversal, subject, predicate, object,
              queryData);
          break;
      }
    }
    // Case 2
    else if (RDF4JHelper.isVariable(subject) && RDF4JHelper.isVariable(predicate)
        && RDF4JHelper.isConcrete(object)) {
      if (RDF4JHelper.isLiteral(object)) {
        TransformOneLengthTriple.getDataProperties(traversal, subject, predicate, object,
            queryData);
      } else {
        TransformOneLengthTriple.getObjectProperties(traversal, subject, predicate, object,
            isInverted, queryData);
      }
    }

    // Case 3
    else if (RDF4JHelper.isVariable(subject) && RDF4JHelper.isConcrete(predicate)
        && RDF4JHelper.isVariable(object)) {
      switch (queryData.getQueryMetaData().getVariableType(object.getName())) {
        case LITERAL:
          TransformOneLengthTriple.getDataProperties(traversal, subject, predicate, object,
              queryData);
          break;
        case NODE_IRI:
          TransformOneLengthTriple.getObjectProperties(traversal, subject, predicate, object,
              isInverted, queryData);
          break;
        case ANY:
          TransformOneLengthTriple.getDataAndObjectProperties(traversal, subject, predicate, object,
              queryData);
          break;
      }
    }
    // Case 4
    else if (RDF4JHelper.isVariable(subject) && RDF4JHelper.isConcrete(predicate)
        && RDF4JHelper.isConcrete(object)) {
      if (RDF4JHelper.isLiteral(object)) {
        TransformOneLengthTriple.getDataProperties(traversal, subject, predicate, object,
            queryData);
      } else {
        TransformOneLengthTriple.getObjectProperties(traversal, subject, predicate, object,
            isInverted, queryData);
      }

    }
    // Case 5
    else if (RDF4JHelper.isIRI(subject) && RDF4JHelper.isVariable(predicate)
        && RDF4JHelper.isVariable(object)) {
      switch (queryData.getQueryMetaData().getVariableType(object.getName())) {
        case LITERAL:
          TransformOneLengthTriple.getDataProperties(traversal, subject, predicate, object,
              queryData);
          break;
        case NODE_IRI:
          TransformOneLengthTriple.getObjectProperties(traversal, subject, predicate, object,
              isInverted, queryData);
          break;
        case ANY:
          TransformOneLengthTriple.getDataAndObjectProperties(traversal, subject, predicate, object,
              queryData);
          break;
      }

    }
    // Case 6
    else if (RDF4JHelper.isIRI(subject) && RDF4JHelper.isVariable(predicate)
        && RDF4JHelper.isConcrete(object)) {
      if (RDF4JHelper.isLiteral(object)) {
        TransformOneLengthTriple.getDataProperties(traversal, subject, predicate, object,
            queryData);
      } else {
        TransformOneLengthTriple.getObjectProperties(traversal, subject, predicate, object,
            isInverted, queryData);
      }
    }

    // Case 7
    else if (RDF4JHelper.isIRI(subject) && RDF4JHelper.isConcrete(predicate)
        && RDF4JHelper.isVariable(object)) {
      switch (queryData.getQueryMetaData().getVariableType(object.getName())) {
        case LITERAL:
          TransformOneLengthTriple.getDataProperties(traversal, subject, predicate, object,
              queryData);
          break;
        case NODE_IRI:
          TransformOneLengthTriple.getObjectProperties(traversal, subject, predicate, object,
              isInverted, queryData);
          break;
        case ANY:
          TransformOneLengthTriple.getDataAndObjectProperties(traversal, subject, predicate, object,
              queryData);
          break;
      }
    }
    // Case 8
    else if (RDF4JHelper.isIRI(subject) && RDF4JHelper.isConcrete(predicate)
        && RDF4JHelper.isConcrete(object)) {
      if (RDF4JHelper.isLiteral(object)) {
        TransformOneLengthTriple.getDataProperties(traversal, subject, predicate, object,
            queryData);
      } else {
        TransformOneLengthTriple.getObjectProperties(traversal, subject, predicate, object,
            isInverted, queryData);
      }
    }
  }

  /**
   * It converts a triple(subject, predicate, object) to equivalent Gremlin traversal when predicate
   * is a object property
   *
   * @param traversal current traversal
   * @param subject subject of the triple
   * @param predicate predicate of the triple
   * @param object object of the triple
   * @param isInverted True if traversal should start from object, False if it should start from
   *        object of triple
   * @param queryData data specific to given query
   */
  public static void getObjectProperties(GraphTraversal<?, ?> traversal, Var subject, Var predicate,
      Var object, boolean isInverted, QueryData queryData) {
    if (isInverted) {
      getReverseObjectProperties(traversal, subject, predicate, object, queryData);
    } else {
      getObjectProperties(traversal, subject, predicate, object, queryData);
    }
  }

  /**
   * It converts a triple(subject, predicate, object) to equivalent Gremlin traversal when predicate
   * is a data type property
   *
   * @param traversal current traversal
   * @param subject subject of the triple
   * @param predicate predicate of the triple
   * @param object object of the triple
   * @param queryData data specific to given query
   */
  public static void getDataProperties(GraphTraversal<?, ?> traversal, Var subject, Var predicate,
      Var object, QueryData queryData) {
    // Invalid case
    if (RDF4JHelper.isVariable(subject) && queryData.getQueryMetaData()
        .getVariableType(subject.getName()) == VariableType.LITERAL) {
      traversal = traversal.limit(0);
      return;
    }

    String subjVar = queryData.getAnonymousVariable(subject);
    if (RDF4JHelper.isIRI(subject)) {
      traversal = traversal.select(subjVar).has("id", RDF4JHelper.getValue(subject.getValue()));
    } else if (RDF4JHelper.isVariable(subject)) {
      traversal =
          traversal.select(subjVar).values("id").is(TextP.containing(":")).as(subject.getName());
    }

    String predVar = queryData.getAnonymousVariable(predicate);
    if (RDF4JHelper.isConcrete(predicate)) {
      traversal = traversal.select(subjVar)
          .properties(RDF4JHelper.getPredicatValue(predicate, true)).as(predVar);
    } else if (RDF4JHelper.isVariable(predicate)) {
      traversal = traversal.select(subjVar).properties().as(predVar);
      traversal = traversal.select(predVar).key().is(TextP.containing(":")).as(predicate.getName());
    }
    // not passing to avoid the cases where further traversal is required so that
    // only vertices are binded to the property and not the strings
    String objVar = queryData.getAnonymousVariable(object);
    traversal = traversal.select(predVar).value().as(objVar);

    if (RDF4JHelper.isLiteral(object)) {
      traversal = traversal.select(objVar).is(RDF4JHelper.getValue(object.getValue()));
    } else if (RDF4JHelper.isVariable(object)) {
      traversal = traversal.select(objVar).as(object.getName());
    }
    // to match current variable with already existing variable
    traversal = traversal.where(__.select(Pop.first, objVar).as(objVar));

  }

  /**
   * It converts a triple(subject, predicate, object) to equivalent Gremlin traversal when predicate
   * is a object property and traversal starts from the subject
   *
   * @param traversal current traversal
   * @param subject subject of the triple
   * @param predicate predicate of the triple
   * @param object object of the triple
   * @param queryData data specific to given query
   */
  public static void getObjectProperties(GraphTraversal<?, ?> traversal, Var subject, Var predicate,
      Var object, QueryData queryData) {
    // Invalid case
    if (RDF4JHelper.isVariable(subject) && queryData.getQueryMetaData()
        .getVariableType(subject.getName()) == VariableType.LITERAL) {
      traversal = traversal.limit(0);
      return;
    }
    String subjVar = queryData.getAnonymousVariable(subject);
    if (RDF4JHelper.isIRI(subject)) {
      traversal = traversal.select(subjVar).has("id", RDF4JHelper.getValue(subject.getValue()));
    } else if (RDF4JHelper.isVariable(subject)) {
      traversal =
          traversal.select(subjVar).values("id").is(TextP.containing(":")).as(subject.getName());
    }

    String predVar = queryData.getAnonymousVariable(predicate);
    if (RDF4JHelper.isConcrete(predicate)) {
      traversal = traversal.select(subjVar).outE(RDF4JHelper.getPredicatValue(predicate, false))
          .as(predVar);
    } else if (RDF4JHelper.isVariable(predicate)) {
      traversal = traversal.select(subjVar).outE().as(predVar);
      traversal = traversal.select(predVar).label().as(predicate.getName());
    }

    String objVar = queryData.getAnonymousVariable(object);
    traversal = traversal.select(predVar).inV().as(objVar);

    if (RDF4JHelper.isIRI(object)) {
      traversal = traversal.select(objVar).has("id", RDF4JHelper.getValue(object.getValue()));
    } else if (RDF4JHelper.isVariable(object)) {
      traversal =
          traversal.select(objVar).values("id").is(TextP.containing(":")).as(object.getName());
    }
    // to match current variable with already existing variable
    traversal = traversal.where(__.select(Pop.first, objVar).as(objVar));

  }

  /**
   * It converts a triple(subject, predicate, object) to equivalent Gremlin traversal when predicate
   * is a object property and traversal starts from the object
   *
   * @param traversal current traversal
   * @param subject subject of the triple
   * @param predicate predicate of the triple
   * @param object object of the triple
   * @param queryData data specific to given query
   */
  public static void getReverseObjectProperties(GraphTraversal<?, ?> traversal, Var subject,
      Var predicate, Var object, QueryData queryData) {
    // Invalid case
    if (RDF4JHelper.isVariable(object)
        && queryData.getQueryMetaData().getVariableType(object.getName()) == VariableType.LITERAL) {
      traversal = traversal.limit(0);
      return;
    }

    String objVar = queryData.getAnonymousVariable(object);
    if (RDF4JHelper.isIRI(object)) {
      traversal = traversal.select(objVar).has("id", RDF4JHelper.getValue(object.getValue()));
    } else if (RDF4JHelper.isVariable(object)) {
      traversal =
          traversal.select(objVar).values("id").is(TextP.containing(":")).as(object.getName());
    }

    String predVar = queryData.getAnonymousVariable(predicate);
    if (RDF4JHelper.isConcrete(predicate)) {
      traversal =
          traversal.select(objVar).inE(RDF4JHelper.getPredicatValue(predicate, false)).as(predVar);
    } else if (RDF4JHelper.isVariable(predicate)) {
      traversal = traversal.select(objVar).inE().as(predVar);
      traversal = traversal.select(predVar).label().as(predicate.getName());
    }

    String subjVar = queryData.getAnonymousVariable(subject);
    traversal = traversal.select(predVar).outV().as(subjVar);

    if (RDF4JHelper.isIRI(subject)) {
      traversal =
          traversal.select(subjVar).values("id").is(RDF4JHelper.getValue(subject.getValue()));
    } else if (RDF4JHelper.isVariable(subject)) {
      traversal =
          traversal.select(subjVar).values("id").is(TextP.containing(":")).as(subject.getName());
    }
    // to match current variable with already existing variable
    traversal = traversal.where(__.select(Pop.first, subjVar).as(subjVar));
  }

  /**
   * It converts a triple(subject, predicate, object) to equivalent Gremlin traversal when predicate
   * can either be a data property or object property
   *
   * @param traversal current traversal
   * @param subject subject of the triple
   * @param predicate predicate of the triple
   * @param object object of the triple
   * @param queryData data specific to given query
   */
  public static void getDataAndObjectProperties(GraphTraversal<?, ?> traversal, Var subject,
      Var predicate, Var object, QueryData queryData) {
    // Invalid case
    if (RDF4JHelper.isVariable(subject) && queryData.getQueryMetaData()
        .getVariableType(subject.getName()) == VariableType.LITERAL) {
      traversal = traversal.limit(0);
      return;
    }

    GraphTraversal<?, ?> dataProp = __.start();
    TransformOneLengthTriple.getDataProperties(dataProp, subject, predicate, object, queryData);

    GraphTraversal<?, ?> objectProp = __.start();
    TransformOneLengthTriple.getObjectProperties(objectProp, subject, predicate, object, queryData);

    traversal = traversal.union((Traversal) dataProp, (Traversal) objectProp);
  }

}
