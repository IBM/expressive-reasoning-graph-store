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

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.Query;
import org.eclipse.rdf4j.query.impl.MapBindingSet;
import com.ibm.research.ergs.query.engine.SPARQLQueryProcessor;

/**
 * SPARQL query for ERGS repository
 *
 * @author Udit Sharma
 *
 */
public abstract class AbstractExpressiveReasoningGraphStoreQuery implements Query {
  protected MapBindingSet bindings = new MapBindingSet();
  protected String queryString;
  protected String baseIRI;
  protected SPARQLQueryProcessor queryProcessor;

  /**
   * It constructs SPARQL query for ERGS repository
   *
   * @param queryProcessor query execution engine
   * @param queryString query string
   * @param baseIRI base IRI for the query
   */
  protected AbstractExpressiveReasoningGraphStoreQuery(SPARQLQueryProcessor queryProcessor,
      String queryString, String baseIRI) {
    this.queryProcessor = queryProcessor;
    this.queryString = queryString;
    this.baseIRI = baseIRI;
  }

  @Override
  public void setBinding(String name, Value value) {
    bindings.addBinding(name, value);

  }

  @Override
  public void removeBinding(String name) {
    bindings.removeBinding(name);

  }

  @Override
  public void clearBindings() {
    bindings.clear();

  }

  @Override
  public BindingSet getBindings() {
    return bindings;
  }

  @Override
  public void setDataset(Dataset dataset) {
    // throw new UnsupportedOperationException();

  }

  @Override
  public Dataset getDataset() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setIncludeInferred(boolean includeInferred) {
    if (!includeInferred) {
      throw new UnsupportedOperationException("Inference can't be disabled.");
    }
  }

  @Override
  public boolean getIncludeInferred() {
    return false;
  }

  @Override
  public void setMaxQueryTime(int maxQueryTime) {
    throw new UnsupportedOperationException();

  }

  @Override
  public int getMaxQueryTime() {
    throw new UnsupportedOperationException();
  }

  protected String getQueryString() {
    if (bindings.size() == 0) {
      return queryString;
    }
    String qry = queryString;
    int b = qry.indexOf('{');
    String select = qry.substring(0, b);
    String where = qry.substring(b);
    for (String name : bindings.getBindingNames()) {
      String replacement = getReplacement(bindings.getValue(name));
      if (replacement != null) {
        String pattern = "[\\?\\$]" + name + "(?=\\W)";
        select = select.replaceAll(pattern, "");
        where = where.replaceAll(pattern, replacement);
      }
    }
    return select + where;
  }

  private String getReplacement(Value value) {
    StringBuilder sb = new StringBuilder();
    if (value instanceof IRI) {
      return appendValue(sb, (IRI) value).toString();
    } else if (value instanceof Literal) {
      return appendValue(sb, (Literal) value).toString();
    } else {
      throw new IllegalArgumentException("BNode references not supported by SPARQL end-points");
    }
  }

  private StringBuilder appendValue(StringBuilder sb, IRI uri) {
    sb.append("<").append(uri.stringValue()).append(">");
    return sb;
  }

  private StringBuilder appendValue(StringBuilder sb, Literal lit) {
    sb.append('"');
    sb.append(lit.getLabel().replace("\"", "\\\""));
    sb.append('"');

    if (lit.getLanguage() != null) {
      sb.append('@');
      sb.append(lit.getLanguage());
    } else if (lit.getDatatype() != null) {
      sb.append("^^<");
      sb.append(lit.getDatatype().stringValue());
      sb.append('>');
    }
    return sb;
  }

}
