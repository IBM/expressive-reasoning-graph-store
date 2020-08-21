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

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.jena.iri.IRIFactory;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.impl.MapBindingSet;

public class ExpressiveReasoningGraphStoreTupleQueryResult implements TupleQueryResult {
  Stream resultStream;
  CloseableIterator<HashMap<String, Value>> resultIterator;
  List<String> queryVariables;

  public ExpressiveReasoningGraphStoreTupleQueryResult(Stream resultStream,
      Set<String> queryVariables) {
    this.resultStream = resultStream;
    this.queryVariables = new ArrayList<>(queryVariables);
    Iterable<HashMap<String, Value>> result = (Iterable<HashMap<String, Value>>) resultStream
        .map(object -> getValueMapping((HashMap<String, Object>) object))::iterator;
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
   * It maps {@link HashMap} with bindings in DB specific representation to representation that can
   * be consumed by user
   *
   * @param original bindings with DB specific representations
   * @return bindings with user oriented representations
   */
  private HashMap<String, Value> getValueMapping(HashMap<String, Object> original) {
    HashMap<String, Value> mapped = new HashMap<String, Value>();
    ValueFactory factory = SimpleValueFactory.getInstance();
    for (String key : original.keySet()) {
      Object value = original.get(key);
      if (value instanceof String && value.equals("N/A")) {
        continue;
      }
      mapped.put(key, getVal(value));
    }
    return mapped;

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
  public BindingSet next() throws QueryEvaluationException {
    HashMap<String, Value> bindings = resultIterator.next();
    MapBindingSet bs = new MapBindingSet(bindings.size());
    bindings.forEach((name, value) -> bs.addBinding(name, value));
    return bs;
  }

  @Override
  public void remove() throws QueryEvaluationException {
    resultIterator.next();
    resultIterator.remove();
  }

  @Override
  public List<String> getBindingNames() throws QueryEvaluationException {
    return queryVariables;
  }

}
