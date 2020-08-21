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
package com.ibm.research.ergs.rdf4j.repository;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import org.eclipse.rdf4j.IsolationLevel;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.CloseableIteratorIteration;
import org.eclipse.rdf4j.common.iteration.Iteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleNamespace;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.BooleanQuery;
import org.eclipse.rdf4j.query.GraphQuery;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.Query;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.Update;
import org.eclipse.rdf4j.query.parser.ParsedBooleanQuery;
import org.eclipse.rdf4j.query.parser.ParsedGraphQuery;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.ParsedTupleQuery;
import org.eclipse.rdf4j.query.parser.QueryParserUtil;
import org.eclipse.rdf4j.queryrender.RenderUtils;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.repository.UnknownTransactionStateException;
import org.eclipse.rdf4j.rio.ParserConfig;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import com.ibm.research.ergs.ingestion.loader.LoadRDFData;
import com.ibm.research.ergs.query.engine.SPARQLQueryProcessor;
import com.ibm.research.ergs.rdf4j.janusgraph.JanusGraphConnection;
import com.ibm.research.ergs.rdf4j.query.ExpressiveReasoningGraphStoreBooleanQuery;
import com.ibm.research.ergs.rdf4j.query.ExpressiveReasoningGraphStoreGraphQuery;
import com.ibm.research.ergs.rdf4j.query.ExpressiveReasoningGraphStoreGraphQueryResult;
import com.ibm.research.ergs.rdf4j.query.ExpressiveReasoningGraphStoreTupleQuery;

/**
 * {@link RepositoryConnection} for ERGS {@link Repository}
 *
 * @author Udit Sharma
 *
 */
public class ExpressiveReasoningGraphStoreRepositoryConnection implements RepositoryConnection {
  private Map<String, String> namespaces;
  private ExpressiveReasoningGraphStoreRepository repository;
  private JanusGraphConnection janusGraphConnection;
  private RDFParser rdfParser;
  private LoadRDFData loadRDFData;
  private SPARQLQueryProcessor queryProcessor;

  /**
   * It creates new {@link RepositoryConnection} for ERGS
   *
   * @param janusGraphConnection {@link JanusGraphConnection}
   * @param repository ERGS {@link Repository}
   */
  ExpressiveReasoningGraphStoreRepositoryConnection(JanusGraphConnection janusGraphConnection,
      ExpressiveReasoningGraphStoreRepository repository) {
    this.namespaces = new HashMap<String, String>();
    this.repository = repository;
    this.janusGraphConnection = janusGraphConnection;
    this.rdfParser = Rio.createParser(RDFFormat.RDFXML, this.repository.getValueFactory());
    this.queryProcessor = janusGraphConnection.getSPARQLQueryProcessor();
    this.loadRDFData = janusGraphConnection.getRDFLoader();

  }

  public void closeConnection() {

  }

  @Override
  public void close() {
    // janusGraphConnection.close();
  }

  @Override
  public void add(Statement st, Resource... contexts) throws RepositoryException {
    loadRDFData.loadRDFStatement(st);
  }

  @Override
  public void add(InputStream in, String baseURI, RDFFormat dataFormat, Resource... contexts)
      throws IOException, RDFParseException, RepositoryException {
    loadRDFData.loadFromInputStream(in, dataFormat, baseURI);
    in.close();
  }

  @Override
  public void add(Reader reader, String baseURI, RDFFormat dataFormat, Resource... contexts)
      throws IOException, RDFParseException, RepositoryException {
    loadRDFData.loadFromReader(reader, dataFormat, baseURI);
    reader.close();
  }

  @Override
  public void add(File file, String baseURI, RDFFormat dataFormat, Resource... contexts)
      throws IOException, RDFParseException, RepositoryException {
    if (file.isDirectory()) {
      loadRDFData.loadDirectoryAllFiles(file, dataFormat, baseURI);
    } else {
      loadRDFData.loadFromFile(file, dataFormat, baseURI);
    }
  }

  @Override
  public void add(URL url, String baseURI, RDFFormat dataFormat, Resource... contexts)
      throws IOException, RDFParseException, RepositoryException {
    // TODO Auto-generated method stub
  }

  @Override
  public void add(Resource subject, IRI predicate, Value object, Resource... contexts)
      throws RepositoryException {
    ValueFactory factory = SimpleValueFactory.getInstance();
    Statement st = factory.createStatement(subject, predicate, object);
    add(st, contexts);
  }

  @Override
  public void add(Iterable<? extends Statement> statements, Resource... contexts)
      throws RepositoryException {
    Iterator<? extends Statement> statementIterator = statements.iterator();
    while (statementIterator.hasNext()) {
      Statement st = statementIterator.next();
      add(st, contexts);
    }
  }

  @Override
  public <E extends Exception> void add(Iteration<? extends Statement, E> statements,
      Resource... contexts) throws RepositoryException, E {
    // TODO Auto-generated method stub
    while (statements.hasNext()) {
      Statement st = statements.next();
      add(st, contexts);
    }

  }

  @Override
  public Repository getRepository() {
    return repository;
  }

  @Override
  public ParserConfig getParserConfig() {
    // TODO Auto-generated method stub
    return rdfParser.getParserConfig();
  }

  @Override
  public RepositoryResult<Resource> getContextIDs() throws RepositoryException {
    LinkedList<Resource> contexts = new LinkedList<Resource>();
    return new RepositoryResult<Resource>(
        new CloseableIteratorIteration<Resource, RepositoryException>(contexts.iterator()));
  }

  @Override
  public void exportStatements(Resource subj, IRI pred, Value obj, boolean includeInferred,
      RDFHandler handler, Resource... contexts) throws RepositoryException, RDFHandlerException {
    RepositoryResult<Statement> stms = getStatements(subj, pred, obj, includeInferred, contexts);
    handler.startRDF();
    // handle
    if (stms != null) {
      while (stms.hasNext()) {
        Statement st = stms.next();
        if (st != null) {
          handler.handleStatement(st);
        }
      }
    }
    handler.endRDF();

  }

  @Override
  public void export(RDFHandler handler, Resource... contexts)
      throws RepositoryException, RDFHandlerException {
    exportStatements(null, null, null, false, handler, contexts);

  }

  @Override
  public IsolationLevel getIsolationLevel() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void begin() throws RepositoryException {
    // TODO Auto-generated method stub

  }

  @Override
  public void begin(IsolationLevel level) throws RepositoryException {
    // TODO Auto-generated method stub

  }

  @Override
  public void commit() throws RepositoryException {
    // graphDatabase.commit();

  }

  @Override
  public void clear(Resource... contexts) throws RepositoryException {
    if (contexts.length == 0 || (contexts.length > 0 && contexts[0] == null)) {
      janusGraphConnection.deleteGraph();
    }
  }

  @Override
  public RepositoryResult<Namespace> getNamespaces() throws RepositoryException {
    Set<Namespace> namespSet = new HashSet<Namespace>();
    for (String key : namespaces.keySet()) {
      // convert into namespace objects
      namespSet.add(new SimpleNamespace(key, namespaces.get(key)));
    }
    return new RepositoryResult<Namespace>(
        new CloseableIteratorIteration<Namespace, RepositoryException>(namespSet.iterator()));
  }

  @Override
  public String getNamespace(String prefix) throws RepositoryException {
    return namespaces.get(prefix);
  }

  @Override
  public void clearNamespaces() throws RepositoryException {
    namespaces.clear();
  }

  @Override
  public RepositoryResult<Statement> getStatements(Resource subj, IRI pred, Value obj,
      boolean includeInferred, Resource... contexts) throws RepositoryException {
    Iterator<Statement> iterator = Collections.emptyIterator();
    // TODO: Handle this case
    // if (pred != null && !propertyMapping.isProperty(pred.stringValue()))
    // {
    // return new RepositoryResult<Statement>(new
    // CloseableIteratorIteration<Statement, RepositoryException>(
    // CloseableIterator.asCloseable(Collections.emptyIterator())));
    // }
    // construct query for it
    StringBuilder queryString = new StringBuilder("CONSTRUCT {");

    StringBuilder spo =
        subj == null ? new StringBuilder("?s ") : RenderUtils.toSPARQL(subj, new StringBuilder());

    spo = pred == null ? spo.append(" ?p ") : RenderUtils.toSPARQL(pred, spo);

    spo = obj == null ? spo.append(" ?o ") : RenderUtils.toSPARQL(obj, spo);

    queryString.append(spo).append("} WHERE {").append(spo).append("}");
    // queryString.append("LIMIT 100");

    // execute construct query
    try {

      if (contexts.length == 0 || (contexts.length > 0 && contexts[0] == null)) {
        GraphQuery query = prepareGraphQuery(QueryLanguage.SPARQL, queryString.toString());
        ExpressiveReasoningGraphStoreGraphQueryResult result =
            (ExpressiveReasoningGraphStoreGraphQueryResult) query.evaluate();
        iterator = result.getIterator();
      }
      CloseableIteration<Statement, RepositoryException> iter =
          new CloseableIteratorIteration<Statement, RepositoryException>(iterator);
      RepositoryResult<Statement> repoResult = new RepositoryResult<Statement>(iter);

      return repoResult;
    } catch (MalformedQueryException e) {
      throw new RepositoryException(e);

    } catch (QueryEvaluationException e) {
      throw new RepositoryException(e);

    }

  }

  @Override
  public ValueFactory getValueFactory() {
    return repository.getValueFactory();
  }

  @Override
  public boolean hasStatement(Resource subj, IRI pred, Value obj, boolean includeInferred,
      Resource... contexts) throws RepositoryException {
    RepositoryResult<Statement> stIter = getStatements(subj, pred, obj, includeInferred, contexts);
    try {
      return stIter.hasNext();
    } finally {
      stIter.close();
    }
  }

  @Override
  public boolean hasStatement(Statement st, boolean includeInferred, Resource... contexts)
      throws RepositoryException {
    return hasStatement(st.getSubject(), st.getPredicate(), st.getObject(), includeInferred,
        contexts);
  }

  @Override
  public boolean isActive() throws UnknownTransactionStateException, RepositoryException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean isAutoCommit() throws RepositoryException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean isEmpty() throws RepositoryException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean isOpen() throws RepositoryException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public Query prepareQuery(QueryLanguage ql, String query)
      throws RepositoryException, MalformedQueryException {
    return prepareQuery(ql, query, "");
  }

  @Override
  public Query prepareQuery(QueryLanguage ql, String query, String baseURI)
      throws RepositoryException, MalformedQueryException {
    if (ql != QueryLanguage.SPARQL) {
      throw new MalformedQueryException("SPARQL query expected! ");
    }

    ParsedQuery q = QueryParserUtil.parseQuery(QueryLanguage.SPARQL, query, baseURI);
    if (q instanceof ParsedTupleQuery) {
      return prepareTupleQuery(ql, query, baseURI);
    } else if (q instanceof ParsedBooleanQuery) {
      return prepareBooleanQuery(ql, query, baseURI);
    } else if (q instanceof ParsedGraphQuery) {
      return prepareGraphQuery(ql, query, baseURI);
    } else {
      throw new MalformedQueryException("Unrecognized query type. " + query);
    }
  }

  @Override
  public TupleQuery prepareTupleQuery(QueryLanguage ql, String query)
      throws RepositoryException, MalformedQueryException {
    return prepareTupleQuery(ql, query, "");
  };

  @Override
  public TupleQuery prepareTupleQuery(QueryLanguage ql, String query, String baseURI)
      throws RepositoryException, MalformedQueryException {
    return new ExpressiveReasoningGraphStoreTupleQuery(queryProcessor, query, baseURI);

  }

  @Override
  public GraphQuery prepareGraphQuery(QueryLanguage ql, String query)
      throws RepositoryException, MalformedQueryException {
    return prepareGraphQuery(ql, query, "");
  }

  @Override
  public GraphQuery prepareGraphQuery(QueryLanguage ql, String query, String baseURI)
      throws RepositoryException, MalformedQueryException {
    return new ExpressiveReasoningGraphStoreGraphQuery(queryProcessor, query, baseURI);
  }

  @Override
  public BooleanQuery prepareBooleanQuery(QueryLanguage ql, String query)
      throws RepositoryException, MalformedQueryException {
    return prepareBooleanQuery(ql, query, "");

  }

  @Override
  public BooleanQuery prepareBooleanQuery(QueryLanguage ql, String query, String baseURI)
      throws RepositoryException, MalformedQueryException {
    return new ExpressiveReasoningGraphStoreBooleanQuery(queryProcessor, query, baseURI);
  }

  @Override
  public Update prepareUpdate(QueryLanguage ql, String update)
      throws RepositoryException, MalformedQueryException {
    return prepareUpdate(ql, update, "");
  }

  @Override
  public Update prepareUpdate(QueryLanguage ql, String update, String baseURI)
      throws RepositoryException, MalformedQueryException {
    throw new RepositoryException("Update not implemented");
  }

  @Override
  public void setParserConfig(ParserConfig config) {
    rdfParser.setParserConfig(config);

  }

  @Override
  public long size(Resource... contexts) throws RepositoryException {
    long size = 0;
    try (
        RepositoryResult<Statement> statements = getStatements(null, null, null, false, contexts)) {
      while (statements.hasNext()) {
        Statement st = statements.next();
        size++;
      }
    }
    return size;
  }

  @Override
  public void setAutoCommit(boolean autoCommit) throws RepositoryException {
    // TODO Auto-generated method stub
  }

  @Override
  public void setIsolationLevel(IsolationLevel level) throws IllegalStateException {
    // TODO Auto-generated method stub
  }

  @Override
  public void rollback() throws RepositoryException {
    // TODO Auto-generated method stub
  }

  @Override
  public void remove(Resource subject, IRI predicate, Value object, Resource... contexts)
      throws RepositoryException {
    throw new RepositoryException("Delete Not implemented");
  }

  @Override
  public void remove(Statement st, Resource... contexts) throws RepositoryException {
    throw new RepositoryException("Delete Not implemented");
  }

  @Override
  public void remove(Iterable<? extends Statement> statements, Resource... contexts)
      throws RepositoryException {
    throw new RepositoryException("Delete Not implemented");
  }

  @Override
  public <E extends Exception> void remove(Iteration<? extends Statement, E> statements,
      Resource... contexts) throws RepositoryException, E {
    throw new RepositoryException("Delete Not implemented");
  }

  @Override
  public void setNamespace(String prefix, String name) throws RepositoryException {
    namespaces.put(prefix, name);
  }

  @Override
  public void removeNamespace(String prefix) throws RepositoryException {
    namespaces.remove(prefix);
  }

}
