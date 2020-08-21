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
package com.ibm.research.ergs.rdf4j.query;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.jena.iri.IRIFactory;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.GraphQueryResult;
import org.eclipse.rdf4j.query.QueryEvaluationException;

/**
 * SELECT query result for ERGS repository
 *
 * @author Udit Sharma
 *
 */
public class ExpressiveReasoningGraphStoreGraphQueryResult implements GraphQueryResult {

  CloseableIterator<Statement> resultIterator;
  Stream resultStream;

  public CloseableIterator<Statement> getIterator() {
    return resultIterator;
  }

  /**
   * It constructs SELECT query results from stream
   *
   * @param resultStream It contains Map<String,Object> of binding for each row
   */
  public ExpressiveReasoningGraphStoreGraphQueryResult(Stream resultStream) {
    super();
    this.resultStream = resultStream;
    Iterable<Statement> result = (Iterable<Statement>) resultStream
        .map(object -> createStatement((HashMap<String, Object>) object))::iterator;
    this.resultIterator = CloseableIterator.asCloseable(result.iterator());
  }

  /**
   * It trims out internal prefixes from the result
   *
   * @param obj string containing IRI/Literal
   * @return IRIs with suffixes trimmed, Literal as it is
   */
  private String trimInternalSuffixes(Object obj) {
    String str = (String) obj;
    if (str.endsWith("_edge")) {
      str = str.substring(0, str.indexOf("_edge"));
    } else if (str.endsWith("_prop")) {
      str = str.substring(0, str.indexOf("_prop"));
    }
    return str;
  }

  /**
   * It checks whether url is valid IRI or not
   *
   * @param url IRI/Literal
   * @return flag
   */
  private boolean isIRI(String url) {
    url = url.toLowerCase();
    return org.semanticweb.owlapi.model.IRI.create(url).isAbsolute()
        && !IRIFactory.iriImplementation().create(url).hasViolation(true);
  }

  /**
   * It converts input object into equivalent {@link Value} object depending upon its type
   *
   * @param obj object of a triple
   * @return Value object
   */
  private Value getVal(Object obj) {
    ValueFactory factory = SimpleValueFactory.getInstance();
    if (obj instanceof List) {
      return getVal(((List) obj).get(0));
    } else if (obj instanceof Long) {
      return factory.createLiteral((Long) obj);
    } else if (obj instanceof Integer) {
      return factory.createLiteral((Integer) obj);
    } else if (obj instanceof Double) {
      return factory.createLiteral((Double) obj);
    } else if (obj instanceof Float) {
      return factory.createLiteral((Float) obj);
    } else if (obj instanceof Date) {
      return factory.createLiteral((Date) obj);
    } else {
      return isIRI((String) obj) ? factory.createIRI(trimInternalSuffixes(obj))
          : factory.createLiteral((String) obj);
    }
  }

  /**
   * It converts link {@link HashMap} into {@link Statement}
   *
   * @param triple bindings for subject, predicate and object
   * @return Statement object
   */
  private Statement createStatement(HashMap<String, Object> triple) {
    ValueFactory factory = SimpleValueFactory.getInstance();
    Resource subject = factory.createIRI(trimInternalSuffixes(triple.get("subject")));
    IRI predicate = factory.createIRI(trimInternalSuffixes(triple.get("predicate")));
    Value object = getVal(triple.get("object"));
    return factory.createStatement(subject, predicate, object);
  }

  @Override
  public void close() throws QueryEvaluationException {
    resultIterator.close();

  }

  @Override
  public boolean hasNext() throws QueryEvaluationException {
    return resultIterator.hasNext();
  }

  @Override
  public Statement next() throws QueryEvaluationException {
    return resultIterator.next();
  }

  @Override
  public void remove() throws QueryEvaluationException {
    resultIterator.next();
    resultIterator.remove();
  }

  @Override
  public Map<String, String> getNamespaces() throws QueryEvaluationException {
    return new HashMap<String, String>();
  }

}
