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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.vocabulary.FN;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.algebra.ArbitraryLengthPath;
import org.eclipse.rdf4j.query.algebra.BindingSetAssignment;
import org.eclipse.rdf4j.query.algebra.Compare;
import org.eclipse.rdf4j.query.algebra.Difference;
import org.eclipse.rdf4j.query.algebra.Exists;
import org.eclipse.rdf4j.query.algebra.Extension;
import org.eclipse.rdf4j.query.algebra.ExtensionElem;
import org.eclipse.rdf4j.query.algebra.Filter;
import org.eclipse.rdf4j.query.algebra.FunctionCall;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.LeftJoin;
import org.eclipse.rdf4j.query.algebra.ListMemberOperator;
import org.eclipse.rdf4j.query.algebra.Not;
import org.eclipse.rdf4j.query.algebra.Regex;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Str;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.UnaryTupleOperator;
import org.eclipse.rdf4j.query.algebra.Union;
import org.eclipse.rdf4j.query.algebra.ValueConstant;
import org.eclipse.rdf4j.query.algebra.ValueExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.ZeroLengthPath;
import org.janusgraph.core.attribute.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.ibm.research.ergs.query.metadata.VariableTypeMetaData.VariableType;
import com.ibm.research.ergs.query.translation.TransformFilter;
import com.ibm.research.ergs.query.utils.CostModel;
import com.ibm.research.ergs.query.utils.RDF4JHelper;

/**
 * This class is responsible to extracting filter that utilises indexes for each TupleExpr
 *
 * @author Udit Sharma
 *
 */
public class IndexedFilterMetaData {

  private static final Logger logger = LoggerFactory.getLogger(IndexedFilterMetaData.class);

  /**
   * These filters are applied after .V() or .E() to utilize the indexes built on graph
   */
  public class IndexedFilter {
    // Identifies if the filter utilizes vertexIndex or EdgeIndex
    private boolean vertexIndex;

    // Use getAnonymous(asVar) with .as()
    private Var asVar;

    // Actual Filter
    private GraphTraversal<?, ?> filter;

    // Rank to identify better filtering condtion
    private long rank;

    public IndexedFilter(boolean vertexIndex, Var asVar, GraphTraversal<?, ?> filter, Long rank) {
      super();
      this.vertexIndex = vertexIndex;
      this.asVar = asVar;
      this.filter = filter;
      this.rank = rank;
    }

    public boolean isVertexIndex() {
      return vertexIndex;
    }

    public Var getAsVar() {
      return asVar;
    }

    public GraphTraversal<?, ?> getFilter() {
      return filter;
    }

    public long getRank() {
      return rank;
    }

    @Override
    public String toString() {
      String ret = "";
      ret += isVertexIndex() ? "V()" : "E()";
      ret += getFilter();
      ret += ".as(" + asVar.getName() + ")";
      ret += rank;
      return ret;

    }
  }

  private PropertyTypeMetaData propertyTypeMetadata;
  private VariableTypeMetaData variableTypeMetadata;
  private VariablesMetaData variablesMetadata;
  private TriplesMetaData triplesMetadata;
  private CostModel costModel;

  private Map<TupleExpr, List<IndexedFilter>> indexedFilterMap;

  /**
   * Constructs {@link IndexedFilterMetaData}
   *
   * @param root root the query parse tree
   * @param costModel {@link CostModel} for db
   * @param propertyTypeMetadata {@link PropertyTypeMetaData} for query
   * @param variablesMetadata {@link VariableTypeMetaData} for the query
   * @param variableTypeMetadata {@link VariablesMetaData} for the query
   * @param literalTriplesMetadata {@link TriplesMetaData} for the query
   */
  public IndexedFilterMetaData(TupleExpr root, CostModel costModel,
      PropertyTypeMetaData propertyTypeMetadata, VariablesMetaData variablesMetadata,
      VariableTypeMetaData variableTypeMetadata, TriplesMetaData literalTriplesMetadata) {
    super();
    this.indexedFilterMap = new HashMap<TupleExpr, List<IndexedFilter>>();
    this.propertyTypeMetadata = propertyTypeMetadata;
    this.triplesMetadata = literalTriplesMetadata;
    this.variableTypeMetadata = variableTypeMetadata;
    this.variablesMetadata = variablesMetadata;
    this.costModel = costModel;
    process(root);
    // System.out.println(indexedFilterMap);
  }

  /**
   * Returns the {@link List} of {@link IndexedFilter} for given {@link TupleExpr}
   *
   * @param tupleExpr expression
   * @return indexed filters for given expression
   */
  public List<IndexedFilter> getIndexedFilter(TupleExpr tupleExpr) {
    return indexedFilterMap.get(tupleExpr);
  }

  /**
   * returns true if a has better indexes than b
   *
   * @param a first TupleExpr
   * @param b second TupleExpr
   * @return true if either there is no indexedFilter available on b or there are better
   *         indexedFitler available for a, false otherwise
   */
  public boolean isBetterIndexed(TupleExpr a, TupleExpr b) {
    List<IndexedFilter> first = getIndexedFilter(a);
    List<IndexedFilter> second = getIndexedFilter(b);
    if (second == null) {
      return true;
    }
    if (first == null) {
      return false;
    }
    long rankFirst = 0;
    long rankSecond = 0;
    for (IndexedFilter filter : first) {
      rankFirst += filter.getRank();
    }
    for (IndexedFilter filter : second) {
      rankSecond += filter.getRank();
    }
    return rankFirst < rankSecond ? true : false;
  }

  /**
   * Populates indexedFilterMap with index that can be utilised for given {@link TupleExpr}
   *
   * @param tupleExpr
   */
  private void process(TupleExpr tupleExpr) {
    if (tupleExpr instanceof Filter) {
      process(((Filter) tupleExpr));
    } else if (tupleExpr instanceof Extension) {
      process(((Extension) tupleExpr));
    } else if (tupleExpr instanceof UnaryTupleOperator) {
      process(((UnaryTupleOperator) tupleExpr));
    } else if (tupleExpr instanceof Difference) {
      process((Difference) tupleExpr);
    } else if (tupleExpr instanceof Join) {
      process((Join) tupleExpr);
    } else if (tupleExpr instanceof LeftJoin) {
      process((LeftJoin) tupleExpr);
    } else if (tupleExpr instanceof Union) {
      process((Union) tupleExpr);
    } else if (tupleExpr instanceof StatementPattern) {
      process((StatementPattern) tupleExpr);
    } else if (tupleExpr instanceof ArbitraryLengthPath) {
      process((ArbitraryLengthPath) tupleExpr);
    } else if (tupleExpr instanceof ZeroLengthPath) {
      process((ZeroLengthPath) tupleExpr);
    } else if (tupleExpr instanceof BindingSetAssignment) {
      process((BindingSetAssignment) tupleExpr);
    } else {
      logger.warn("Unsupported expression: " + tupleExpr);
    }
  }

  /**
   * Populates indexedFilterMap with index that can be utilised for given {@link Filter} expression
   *
   * @param tupleExpr
   */
  private void process(Filter tupleExpr) {
    process(tupleExpr.getArg());
    // Use better index from condition and argument
    List<IndexedFilter> filters =
        better(process(tupleExpr.getCondition()), indexedFilterMap.get(tupleExpr.getArg()));
    if (filters != null) {
      indexedFilterMap.put(tupleExpr, filters);
    }
  }

  /**
   * Populates indexedFilterMap with index that can be utilised for given {@link UnaryTupleOperator}
   * expression
   *
   * @param tupleExpr
   */
  private void process(UnaryTupleOperator tupleExpr) {
    process(tupleExpr.getArg());
    // Use the index obtained from the argument
    indexedFilterMap.put(tupleExpr, indexedFilterMap.get(tupleExpr.getArg()));
  }

  /**
   * Populates indexedFilterMap with index that can be utilised for given {@link Difference}
   * expression
   *
   * @param tupleExpr
   */
  private void process(Difference tupleExpr) {
    // Don't find indexed filters on the rightArg
    process(tupleExpr.getLeftArg());
    // Use index for the left argument
    indexedFilterMap.put(tupleExpr, indexedFilterMap.get(tupleExpr.getLeftArg()));
  }

  /**
   * Populates indexedFilterMap with index that can be utilised for given {@link Union} expression
   *
   * @param tupleExpr
   */
  private void process(Union tupleExpr) {
    process(tupleExpr.getRightArg());
    process(tupleExpr.getLeftArg());
    // Use both indexes from left and right argument
    indexedFilterMap.put(tupleExpr, merge(indexedFilterMap.get(tupleExpr.getLeftArg()),
        indexedFilterMap.get(tupleExpr.getRightArg())));
  }

  /**
   * Populates indexedFilterMap with index that can be utilised for given {@link LeftJoin}
   * expression
   *
   * @param tupleExpr
   */
  private void process(LeftJoin tupleExpr) {
    process(tupleExpr.getRightArg());
    process(tupleExpr.getLeftArg());
    // Use index for right argument
    indexedFilterMap.put(tupleExpr, indexedFilterMap.get(tupleExpr.getLeftArg()));
  }

  /**
   * Populates indexedFilterMap with index that can be utilised for given {@link Join} expression
   *
   * @param tupleExpr
   */
  private void process(Join tupleExpr) {
    process(tupleExpr.getRightArg());
    process(tupleExpr.getLeftArg());
    // If the variables in left and right argument are disjoint-> use both indexes
    // else -> use better index
    if (Collections.disjoint(variablesMetadata.extractVariables(tupleExpr.getLeftArg()),
        variablesMetadata.extractVariables(tupleExpr.getRightArg()))) {
      indexedFilterMap.put(tupleExpr, merge(indexedFilterMap.get(tupleExpr.getLeftArg()),
          indexedFilterMap.get(tupleExpr.getRightArg())));
    } else {
      indexedFilterMap.put(tupleExpr, better(indexedFilterMap.get(tupleExpr.getLeftArg()),
          indexedFilterMap.get(tupleExpr.getRightArg())));
    }
  }

  /**
   * Populates indexedFilterMap with index that can be utilised for given {@link StatementPattern}
   * expression
   *
   * @param tupleExpr
   */
  private void process(StatementPattern tupleExpr) {
    Var subject = tupleExpr.getSubjectVar();
    Var object = tupleExpr.getObjectVar();
    Var predicate = tupleExpr.getPredicateVar();
    // Use better index from the index available for subject, predicate and object
    List<IndexedFilter> filter = getIndexedFilterForSubject(subject, predicate, object);
    filter = better(filter, getIndexedFilterForObject(subject, predicate, object));
    filter = better(filter, getIndexedFilterForPredicate(subject, predicate, object));
    indexedFilterMap.put(tupleExpr, filter);

  }

  /**
   * Populates indexedFilterMap with index that can be utilised for given
   * {@link ArbitraryLengthPath} expression
   *
   * @param tupleExpr
   */
  private void process(ArbitraryLengthPath tupleExpr) {
    TupleExpr pathExpression = tupleExpr.getPathExpression();
    process(pathExpression);
    // Use indexes for the path expression
    indexedFilterMap.put(tupleExpr, indexedFilterMap.get(pathExpression));
  }

  /**
   * Populates indexedFilterMap with index that can be utilised for given {@link ZeroLengthPath}
   * expression
   *
   * @param tupleExpr
   */
  private void process(ZeroLengthPath tupleExpr) {
    Var subject = tupleExpr.getSubjectVar();
    Var object = tupleExpr.getObjectVar();
    Var predicate = null;
    // Use better index from the index available for subject and object
    List<IndexedFilter> filter = getIndexedFilterForSubject(subject, predicate, object);
    filter = better(filter, getIndexedFilterForObject(subject, predicate, object));
    indexedFilterMap.put(tupleExpr, filter);
  }

  /**
   * Populates indexedFilterMap with index that can be utilised for given {@link Extension}
   * expression
   *
   * @param tupleExpr
   */
  private void process(Extension tupleExpr) {
    boolean isIndexed = true;
    for (ExtensionElem elem : tupleExpr.getElements()) {
      boolean isCurrentIndexed = false;
      String asVar = elem.getName();
      if (elem.getExpr() instanceof ValueConstant) {
        String exprVar = ((ValueConstant) elem.getExpr()).getValue().stringValue();
        // check if we can apply indexes
        if (canApplyIndexedFilter(new Var(asVar), Collections.singletonList(exprVar))) {
          isCurrentIndexed = true;
        }
      }
      isIndexed &= isCurrentIndexed;
    }
    // If bindings present on all the ExtensionElement can be utilised by indexes->
    // add the indexedFitler, Else->ignore
    if (isIndexed) {
      List<IndexedFilter> filters = new ArrayList<IndexedFilter>();
      for (ExtensionElem elem : tupleExpr.getElements()) {
        String asVar = elem.getName();
        String exprVar = ((ValueConstant) elem.getExpr()).getValue().stringValue();
        filters.addAll(getWithinFilter(new Var(asVar), Collections.singletonList(exprVar)));
      }
      indexedFilterMap.put(tupleExpr, filters);
    }
    process(tupleExpr.getArg());
  }

  /**
   * Populates indexedFilterMap with index that can be utilised for given {@link Extension}
   * expression
   *
   * @param tupleExpr
   */
  private void process(BindingSetAssignment tupleExpr) {
    Map<String, List<Object>> bindingsList = new HashMap<String, List<Object>>();
    boolean isIndexed = true;
    for (String name : tupleExpr.getBindingNames()) {
      bindingsList.put(name, new ArrayList<Object>());
    }
    // Check if all the bindingSets are indexed
    for (BindingSet bs : tupleExpr.getBindingSets()) {
      boolean isCurrentIndexed = false;
      // Check if any of the bindings in the bindingSet is indexed
      for (String name : tupleExpr.getBindingNames()) {
        if (bs.getValue(name) != null && canApplyIndexedFilter(new Var(name),
            Collections.singletonList(bs.getValue(name).stringValue()))) {
          bindingsList.get(name).add(bs.getValue(name).stringValue());
          isCurrentIndexed = true;
          break;
        }
      }
      isIndexed &= isCurrentIndexed;

    }
    // If bindings present on all the BindingSet can be utilised by indexes-> add
    // the indexedFitler, Else->ignore
    if (isIndexed) {
      List<IndexedFilter> filters = new ArrayList<IndexedFilter>();
      for (String name : bindingsList.keySet()) {
        List<Object> list = bindingsList.get(name);
        // if (!list.isEmpty())
        List<IndexedFilter> filter = getWithinFilter(new Var(name), list);
        if (filter != null) {
          filters.addAll(filter);
        }
      }
      indexedFilterMap.put(tupleExpr, filters);
    }

  }

  /**
   * Checks whether index is available for given term
   *
   * @param var asVar
   * @param value term to search in index
   * @return True if index is available else False
   */
  private boolean canApplyIndexedFilter(Var var, Object value) {
    // Index for finding node with given IRI
    if (variableTypeMetadata.getVariableType(var.getName()) == VariableType.NODE_IRI) {
      return true;
    } else if (variableTypeMetadata.getVariableType(var.getName()) == VariableType.PREDICATE_IRI) {
      TupleExpr expr = triplesMetadata.getTriples(var);
      if (expr != null) {
        return true;
      }
      // Index for data properties stored on node
    } else if (variableTypeMetadata.getVariableType(var.getName()) == VariableType.LITERAL) {
      StatementPattern expr = triplesMetadata.getTriples(var);
      if (expr != null) {
        Var predicate = expr.getPredicateVar();
        // if predicate is indexed
        if (propertyTypeMetadata.isIndexed(predicate.getValue().stringValue())) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Returns the best index for a list of terms
   *
   * @param var asVar
   * @param values term to search in index
   * @return {@link List} of {@link IndexedFilter}
   */
  private List<IndexedFilter> getWithinFilter(Var var, List<Object> values) {
    // If var is Node IRI use has("id",iri) index
    if (variableTypeMetadata.getVariableType(var.getName()) == VariableType.NODE_IRI) {
      // calculate index cost
      long rank = Math.max(
          costModel.getRelationshipDomainCardinalityForFixedRange(String.valueOf((Object) null),
              values),
          costModel.getRelationshipRangeCardinalityForFixedDomain(String.valueOf((Object) null),
              values));
      return Collections
          .singletonList(new IndexedFilter(true, var, __.has("id", P.within(values)), rank));
    }
    // If var is Predicate IRI used has("label_prop",iri)/ has("label_edge",iri)
    // index
    else if (variableTypeMetadata.getVariableType(var.getName()) == VariableType.PREDICATE_IRI) {
      StatementPattern expr = triplesMetadata.getTriples(var);
      if (expr != null) {
        Var subject = expr.getSubjectVar();
        // calculate cost for all the data properties
        long rankdp =
            values.stream().map(value -> costModel.getPropertyCardinality(String.valueOf(value)))
                .collect(Collectors.summingLong(Long::longValue));
        // calculate cost for all the object properties
        long rankop = values.stream()
            .map(value -> costModel.getRelationshipDomainCardinality(String.valueOf(value)))
            .collect(Collectors.summingLong(Long::longValue));
        // obtain all the data properties
        List<String> dp = values.stream().map(value -> String.valueOf(value) + "_prop")
            .collect(Collectors.toList());
        // obtain all the objec properties
        List<String> op = values.stream().map(value -> String.valueOf(value) + "_edge")
            .collect(Collectors.toList());
        return Arrays.asList(
            new IndexedFilter(true, subject, __.has("label_prop", P.within(dp)), rankdp),
            new IndexedFilter(true, subject, __.has("label_edge", P.within(op)), rankop));
      }
    }
    // If var type is Literal use has(predicate,literal) index
    else if (variableTypeMetadata.getVariableType(var.getName()) == VariableType.LITERAL) {
      StatementPattern expr = triplesMetadata.getTriples(var);
      if (expr != null) {
        Var subject = expr.getSubjectVar();
        Var predicate = expr.getPredicateVar();
        // calculate index cost
        long rank = costModel.getPropertyCardinalityForPredicate(
            String.valueOf(RDF4JHelper.getValue(predicate.getValue())), P.within(values));
        if (propertyTypeMetadata.isIndexed(predicate.getValue().stringValue())) {
          return Collections.singletonList(new IndexedFilter(true, subject,
              __.has(RDF4JHelper.getPredicatValue(predicate, true), P.within(values)), rank));
        }
      }
    }
    // return null if index can't be utilised
    return null;
  }

  /**
   * Returns the best index for a given regular expression
   *
   * @param var asVar
   * @param values term to search in index
   * @return {@link List} of {@link IndexedFilter}
   */
  private List<IndexedFilter> getRegexFilter(Var var, String regex) {
    // var type must be literal
    if (variableTypeMetadata.getVariableType(var.getName()) == VariableType.LITERAL) {
      StatementPattern expr = triplesMetadata.getTriples(var);
      if (expr != null) {
        Var subject = expr.getSubjectVar();
        Var predicate = expr.getPredicateVar();
        // calculate index cost
        long rank = costModel.getPropertyCardinalityForPredicate(
            String.valueOf(RDF4JHelper.getValue(predicate.getValue())), Text.textRegex(regex));
        if (propertyTypeMetadata.isIndexed(predicate.getValue().stringValue())) {
          return Collections.singletonList(new IndexedFilter(true, subject,
              __.has(RDF4JHelper.getPredicatValue(predicate, true), Text.textRegex(regex)), rank));
        }
      }
    }
    // index can't be utilised
    return null;
  }

  /**
   * Returns the indexes that can be utilised if we start the traversal with subject
   *
   * @param subject subject of triple
   * @param predicate predicate of triple
   * @param object object of triple
   * @return {@link List} of {@link IndexedFilter}
   */
  private List<IndexedFilter> getIndexedFilterForSubject(Var subject, Var predicate, Var object) {
    if (RDF4JHelper.isIRI(subject)) {
      long rank;
      Object sub = RDF4JHelper.getValue(subject.getValue());
      Object pred = (RDF4JHelper.isNull(predicate) || RDF4JHelper.isVariable(predicate)) ? null
          : RDF4JHelper.getValue(predicate.getValue());
      // If object is variable
      if (RDF4JHelper.isVariable(object)) {
        // If object type is literal the current node has all the information
        if (variableTypeMetadata.getVariableType(object.getName()) == VariableType.LITERAL) {
          rank = costModel.getMinCardinality();
        }
        // If object type is not a literal we need to fetch all the outgoing nodes
        else {
          rank = costModel.getRelationshipRangeCardinalityForFixedDomain(String.valueOf(pred),
              String.valueOf(sub)) + costModel.getMinCardinality();
        }
      }
      // If object is concrete
      else {
        // If object type is literal the current node has all the information
        if (RDF4JHelper.isLiteral(object)) {
          rank = costModel.getMinCardinality();
        }
        // If object type is not a literal we need to fetch all the outgoing nodes with
        // given label
        else {
          rank = costModel.getRelationshipRangeCardinalityForFixedDomain(String.valueOf(pred),
              String.valueOf(sub));
        }
      }
      return Collections
          .singletonList(new IndexedFilter(true, subject, __.has("id", P.eq(sub)), rank));
    }
    // index can't be utilised
    return Collections
        .singletonList(new IndexedFilter(true, subject, null, costModel.getMaxCardinality()));
  }

  /**
   * Returns the indexes that can be utilised if we start the traversal with object
   *
   * @param subject subject of triple
   * @param predicate predicate of triple
   * @param object object of triple
   * @return {@link List} of {@link IndexedFilter}
   */
  private List<IndexedFilter> getIndexedFilterForObject(Var subject, Var predicate, Var object) {
    // object must be IRI
    if (RDF4JHelper.isIRI(object)) {
      Object obj = RDF4JHelper.getValue(object.getValue());
      Object pred = (RDF4JHelper.isNull(predicate) || RDF4JHelper.isVariable(predicate)) ? null
          : RDF4JHelper.getValue(predicate.getValue());
      // We need to fetch all the incoming nodes with given label
      long rank = costModel.getRelationshipDomainCardinalityForFixedRange(String.valueOf(pred),
          String.valueOf(obj));
      return Collections
          .singletonList(new IndexedFilter(true, object, __.has("id", P.eq(obj)), rank));
    }
    // index can't be utilised
    return Collections
        .singletonList(new IndexedFilter(true, subject, null, costModel.getMaxCardinality()));
  }

  /**
   * Returns the indexes that can be utilised if we start the traversal with predicate
   *
   * @param subject subject of triple
   * @param predicate predicate of triple
   * @param object object of triple
   * @return {@link List} of {@link IndexedFilter}
   */
  private List<IndexedFilter> getIndexedFilterForPredicate(Var subject, Var predicate, Var object) {
    // predicate must be IRI
    if (RDF4JHelper.isIRI(predicate)) {
      Object pred = RDF4JHelper.getValue(predicate.getValue());
      // If object is literal use has(pred,literal) index
      if (RDF4JHelper.isLiteral(object)) {
        Object obj = RDF4JHelper.getValue(object.getValue());
        // calculate index cost
        long rank = costModel.getPropertyCardinalityForExactMatch(String.valueOf(pred),
            String.valueOf(obj));
        if (propertyTypeMetadata.isIndexed(String.valueOf(pred))) {
          return Collections.singletonList(new IndexedFilter(true, subject,
              __.has(RDF4JHelper.getPredicatValue(predicate, true), P.eq(obj)), rank));
        }
      }
      // If object is variable use has('label_prop',pred)/has('label_edge',pred)
      // index
      else if (RDF4JHelper.isVariable(object)) {
        // if object type is LIteral used has('label_prop',pred)
        if (variableTypeMetadata.getVariableType(object.getName()) == VariableType.LITERAL) {
          long rank = costModel.getPropertyCardinality(String.valueOf(pred));
          return Collections.singletonList(new IndexedFilter(true, subject,
              __.has("label_prop", P.eq(RDF4JHelper.getPredicatValue(predicate, true))), rank));
        }
        // if object type is Node IRI used has('label_edge',pred)
        else if (variableTypeMetadata.getVariableType(object.getName()) == VariableType.NODE_IRI) {
          long rank = costModel.getRelationshipDomainCardinality(String.valueOf(pred));
          return Collections.singletonList(new IndexedFilter(true, subject,
              __.has("label_edge", P.eq(RDF4JHelper.getPredicatValue(predicate, false))), rank));
        }
        // else use both has('label_prop',pred) and has('label_edge',pred)
        else {
          long rank1 = costModel.getPropertyCardinality(String.valueOf(pred));
          long rank2 = costModel.getRelationshipDomainCardinality(String.valueOf(pred));
          return Arrays.asList(
              new IndexedFilter(true, subject,
                  __.has("label_prop", RDF4JHelper.getPredicatValue(predicate, true)), rank1),
              new IndexedFilter(true, subject,
                  __.has("label_edge", RDF4JHelper.getPredicatValue(predicate, false)), rank2));
        }
      }
    }
    // index can't be utilised
    return Collections
        .singletonList(new IndexedFilter(true, subject, null, costModel.getMaxCardinality()));
  }

  /**
   * Returns index that can be utilised for given {@link ValueExpr} expression
   *
   * @param tupleExpr
   */
  private List<IndexedFilter> process(ValueExpr valueExpr) {
    if (valueExpr instanceof Compare) {
      return process((Compare) valueExpr);
    } else if (valueExpr instanceof Regex) {
      return process((Regex) valueExpr);
    } else if (valueExpr instanceof ListMemberOperator) {
      return process((ListMemberOperator) valueExpr);
    } else if (valueExpr instanceof FunctionCall) {
      return process((FunctionCall) valueExpr);
    } else if (valueExpr instanceof Exists) {
      process((Exists) valueExpr);
    } else if (valueExpr instanceof Not) {
      process((Not) valueExpr);
    } else {
      // TODO handle other cases
      logger.warn("unsupported expression: " + valueExpr);
    }
    return null;
  }

  /**
   * Returns index that can be utilised for given {@link Exists} expression
   *
   * @param tupleExpr
   */
  private List<IndexedFilter> process(Exists condition) {
    process(condition.getSubQuery());
    return null;
  }

  /**
   * Returns index that can be utilised for given {@link Not} expression
   *
   * @param tupleExpr
   */
  private List<IndexedFilter> process(Not condition) {
    if (condition.getArg() instanceof Exists) {
      return process(condition.getArg());
    }
    return null;
  }

  /**
   * Returns index that can be utilised for given {@link Compare} expression
   *
   * @param tupleExpr
   */
  private List<IndexedFilter> process(Compare condition) {
    ValueExpr left = condition.getLeftArg();
    ValueExpr right = condition.getRightArg();
    // use has(pred,constant) index
    if (left instanceof ValueConstant && right instanceof Var) {
      Var var = ((Var) right);
      Object constant = RDF4JHelper.getValue(((ValueConstant) left).getValue());
      switch (condition.getOperator()) {
        case EQ:
          return getWithinFilter(var, Collections.singletonList(constant));
      }
    } else if (left instanceof Var && right instanceof ValueConstant) {
      Var var = ((Var) left);
      Object constant = RDF4JHelper.getValue(((ValueConstant) right).getValue());
      switch (condition.getOperator()) {
        case EQ:
          return getWithinFilter(var, Collections.singletonList(constant));
      }
    }
    // Can't utilise index
    return null;
  }

  /**
   * Returns index that can be utilised for given {@link Regex} expression
   *
   * @param tupleExpr
   */
  private List<IndexedFilter> process(Regex regex) {
    Var var = null;
    if (regex.getArg() instanceof Str) {
      var = (Var) ((Str) regex.getArg()).getArg();
    } else {
      var = (Var) regex.getArg();
    }
    String pattern = ((ValueConstant) regex.getPatternArg()).getValue().stringValue();
    String flagString = "";
    if (regex.getFlagsArg() != null) {
      if (((ValueConstant) regex.getFlagsArg()).getValue().stringValue().contains("i")) {
        pattern = TransformFilter.getCaseInsensitiveRegex(pattern);
      }
    }
    String newPattern = ".*" + pattern + ".*";
    // use Regex index
    return getRegexFilter(var, newPattern);
  }

  /**
   * Returns index that can be utilised for given {@link ListMemberOperator} expression
   *
   * @param tupleExpr
   */
  private List<IndexedFilter> process(ListMemberOperator operator) {
    Var var = null;
    List<Object> values = new ArrayList<Object>();
    for (ValueExpr expr : operator.getArguments()) {
      if (expr instanceof Var) {
        var = ((Var) expr);
      } else {
        Value value = ((ValueConstant) expr).getValue();
        values.add(RDF4JHelper.getValue(value));
      }
    }
    // use within index
    return getWithinFilter(var, values);
  }

  /**
   * Returns index that can be utilised for given {@link FunctionCall} expression
   *
   * @param tupleExpr
   */
  private List<IndexedFilter> process(FunctionCall function) {
    Var var = null;
    List<String> values = new ArrayList<String>();
    for (ValueExpr expr : function.getArgs()) {
      if (expr instanceof Str) {
        expr = ((Str) expr).getArg();
      }
      if (expr instanceof Var) {
        var = ((Var) expr);
      } else {
        Value value = ((ValueConstant) expr).getValue();
        values.add(value.stringValue());
      }
    }
    assert values.size() == 1;
    if (function.getURI().equals(FN.CONTAINS.stringValue())) {
      return getRegexFilter(var, ".*" + values.get(0) + ".*");
    } else if (function.getURI().equals(FN.ENDS_WITH.stringValue())) {
      return getRegexFilter(var, values.get(0) + ".*");
    } else if (function.getURI().equals(FN.STARTS_WITH.stringValue())) {
      return getRegexFilter(var, ".*" + values.get(0));
    }
    // can't utilise index
    return null;
  }

  /**
   * Merges two lists containing the indexed Filters
   *
   * @param first First List
   * @param second second List
   * @return Merged List
   */
  private List<IndexedFilter> merge(List<IndexedFilter> first, List<IndexedFilter> second) {
    if (first == null || second == null) {
      return null;
    }
    return Stream.concat(first.stream(), second.stream()).collect(Collectors.toList());
  }

  /**
   * Finds better Indexed Filter
   *
   * @param first First list
   * @param second Second list
   * @return Merged list
   */
  private List<IndexedFilter> better(List<IndexedFilter> first, List<IndexedFilter> second) {
    if (first == null) {
      return second;
    } else if (second == null) {
      return first;
    }
    long rankFirst = 0;
    long rankSecond = 0;
    for (IndexedFilter filter : first) {
      rankFirst += filter.getRank();
    }
    for (IndexedFilter filter : second) {
      rankSecond += filter.getRank();
    }
    return rankFirst < rankSecond ? first : second;
  }
}
