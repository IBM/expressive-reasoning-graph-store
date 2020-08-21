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

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.ZeroLengthPath;
import com.ibm.research.ergs.query.utils.RDF4JHelper;

/**
 * This class is responsible to translating Zero length property path expressions in SPARQL query to
 * equivalent Gremlin traversals
 *
 * @author Udit Sharma
 *
 */
public class TransformZeroLengthTriple {

  /**
   * It converts Zero length property path expression
   *
   * @param traversal current traversal
   * @param triple triple pattern
   * @param isInverted True if traversal should start from object, False if it should start from
   *        object of triple
   * @param queryData data specific to given query
   */
  public static void transform(GraphTraversal<?, ?> traversal, ZeroLengthPath triple,
      boolean isInverted, QueryData queryData) {
    Var subject = triple.getSubjectVar();
    Var object = triple.getObjectVar();
    if (isInverted) {
      copy(traversal, object, subject, queryData);
    } else {
      copy(traversal, subject, object, queryData);
    }
  }

  /**
   * it copies the contents of subject to object
   *
   * @param traversal current traversal
   * @param subject subject of the triple
   * @param object object of the triple
   * @param queryData data specific to given query
   */
  public static void copy(GraphTraversal<?, ?> traversal, Var subject, Var object,
      QueryData queryData) {
    String subjVar = queryData.getAnonymousVariable(subject);
    if (RDF4JHelper.isIRI(subject)) {
      traversal = traversal.select(subjVar).has("id", RDF4JHelper.getValue(subject.getValue()));
    } else if (RDF4JHelper.isVariable(subject)) {
      traversal = traversal.select(subjVar).values("id").as(subject.getName());
    }

    String objVar = queryData.getAnonymousVariable(object);
    traversal = traversal.select(subjVar).as(objVar);

    if (RDF4JHelper.isIRI(object)) {
      traversal = traversal.select(objVar).has("id", RDF4JHelper.getValue(object.getValue()));
    } else if (RDF4JHelper.isVariable(object)) {
      traversal = traversal.select(objVar).values("id").as(object.getName());
    }
  }
}
