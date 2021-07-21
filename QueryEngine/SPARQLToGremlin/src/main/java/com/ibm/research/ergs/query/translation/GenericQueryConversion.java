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
package com.ibm.research.ergs.query.translation;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.FN;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.algebra.AggregateOperator;
import org.eclipse.rdf4j.query.algebra.ArbitraryLengthPath;
import org.eclipse.rdf4j.query.algebra.BindingSetAssignment;
import org.eclipse.rdf4j.query.algebra.DescribeOperator;
import org.eclipse.rdf4j.query.algebra.Difference;
import org.eclipse.rdf4j.query.algebra.Distinct;
import org.eclipse.rdf4j.query.algebra.Extension;
import org.eclipse.rdf4j.query.algebra.ExtensionElem;
import org.eclipse.rdf4j.query.algebra.Filter;
import org.eclipse.rdf4j.query.algebra.FunctionCall;
import org.eclipse.rdf4j.query.algebra.Group;
import org.eclipse.rdf4j.query.algebra.GroupElem;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.LeftJoin;
import org.eclipse.rdf4j.query.algebra.MathExpr;
import org.eclipse.rdf4j.query.algebra.MultiProjection;
import org.eclipse.rdf4j.query.algebra.Order;
import org.eclipse.rdf4j.query.algebra.OrderElem;
import org.eclipse.rdf4j.query.algebra.Projection;
import org.eclipse.rdf4j.query.algebra.ProjectionElem;
import org.eclipse.rdf4j.query.algebra.Reduced;
import org.eclipse.rdf4j.query.algebra.Service;
import org.eclipse.rdf4j.query.algebra.SingletonSet;
import org.eclipse.rdf4j.query.algebra.Slice;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.Union;
import org.eclipse.rdf4j.query.algebra.ValueConstant;
import org.eclipse.rdf4j.query.algebra.ValueExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.ZeroLengthPath;
import org.eclipse.rdf4j.query.impl.ListBindingSet;
import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.eclipse.rdf4j.query.parser.QueryParserUtil;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.sparql.SPARQLRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.ibm.research.ergs.query.metadata.IndexedFilterMetaData.IndexedFilter;
import com.ibm.research.ergs.query.metadata.MetaData;
import com.ibm.research.ergs.query.process.DeclarativeQueryProcessor;
import com.ibm.research.ergs.query.utils.CostModel;
import com.ibm.research.ergs.query.utils.PropertyMapping;
import com.ibm.research.ergs.query.utils.RDF4JHelper;

/**
 * This Class is responsible for converting any SPARQL query to equivalent Gremlin query
 *
 * @author Udit Sharma
 *
 */
public class GenericQueryConversion {
  private static final Logger logger = LoggerFactory.getLogger(GenericQueryConversion.class);

  private TupleExpr queryRoot;
  private GraphTraversalSource g;
  private DeclarativeQueryProcessor declarativeQueryPreprocessor;
  private QueryData queryData;

  /**
   * Constructs {@link GenericQueryConversion}
   *
   * @param g Gremlin traversal source
   * @param queryRoot root of SPARQL query parse tree
   * @param propertyMapping {@link PropertyMapping} for fetching type, indexing, existence
   *        information for properties in DB.
   * @param costModel {@link CostModel} for calculating cost for different expressions in a query
   */
  public GenericQueryConversion(GraphTraversalSource g, TupleExpr queryRoot,
      PropertyMapping propertyMapping, CostModel costModel) {
    this.g = g;
    this.queryRoot = queryRoot;
    MetaData metadata = new MetaData(queryRoot, propertyMapping, costModel);
    this.declarativeQueryPreprocessor = new DeclarativeQueryProcessor(metadata);
    this.queryData = new QueryData(metadata, this);
  }

  /**
   * Constructs {@link GenericQueryConversion}
   *
   * @param g Gremlin traversal source
   * @param sparqlQueryString String containing SPARQL query
   * @param propertyMapping {@link PropertyMapping} for fetching type, indexing, existence
   *        information for properties in DB.
   * @param costModel {@link CostModel} for calculating cost for different expressions in a query
   */
  public GenericQueryConversion(GraphTraversalSource g, String sparqlQueryString,
      PropertyMapping propertyMapping, CostModel costModel) {
    this(g,
        QueryParserUtil.parseQuery(QueryLanguage.SPARQL, sparqlQueryString, null).getTupleExpr(),
        propertyMapping, costModel);
  }

  /**
   * Returns equivalent Gremlin traversal for given SPARQL query
   *
   * @return Gremlin traversal
   */
  public GraphTraversal<?, ?> getGremlinTraversal() {
    GraphTraversal<?, ?> traversal = g.inject(1);
    translate(this.queryRoot, traversal, new HashSet<Var>());
    return traversal;
  }

  /**
   *
   * @return {@link Set} of variables in query
   */
  public Set<String> getVariables() {
    return queryData.getQueryVariables();
  }

  /**
   * This method uses reflection to invoke appropriate method depending upon the class of tupleExpr
   *
   * @param tupleExpr root of current parse tree
   * @param traversal current Gremlin traversal
   * @param cur {@link Set} of variables already visited by current traversal
   */
  public void translate(TupleExpr tupleExpr, GraphTraversal<?, ?> traversal, Set<Var> cur) {
    Class cls = GenericQueryConversion.class;
    try {
      Method methodcall =
          cls.getDeclaredMethod("translate", tupleExpr.getClass(), GraphTraversal.class, Set.class);
      methodcall.invoke(this, tupleExpr, traversal, cur);
    } catch (IllegalArgumentException | NoSuchMethodException | SecurityException
        | IllegalAccessException | InvocationTargetException e) {
      logger.error(e.getMessage(), e);
    }
  }

  /**
   * This method converts the parse tree with {@link Reduced} operator at the root
   *
   * {@see <a href= "https://www.w3.org/TR/sparql11-query/#modDuplicates">Reduced</a>}
   *
   * @param tupleExpr subtree of the parse tree corresponding to the actual query with
   *        {@link Reduced} at the root
   * @param traversal current gremlin traversal
   * @param cur {@link Set} of variables already visited by current traversal
   */
  private void translate(Reduced tupleExpr, GraphTraversal<?, ?> traversal, Set<Var> cur) {
    translate(tupleExpr.getArg(), traversal, cur);
  }

  /**
   * This method converts the parse tree with {@link DescribeOperator} operator at the root
   *
   * {@see <a href= "https://www.w3.org/TR/sparql11-query/#describe">Describe</a>}
   *
   * @param tupleExpr subtree of the parse tree corresponding to the actual query with
   *        {@link DescribeOperator} at the root
   * @param traversal current gremlin traversal
   * @param cur {@link Set} of variables already visited by current traversal
   */
  private void translate(DescribeOperator tupleExpr, GraphTraversal<?, ?> traversal, Set<Var> cur) {
    translate(tupleExpr.getArg(), traversal, cur);

    String[] describeVars = tupleExpr.getAssuredBindingNames().toArray(new String[0]);
    GraphTraversal<?, Map<String, Object>>[] unionTraversals =
        new GraphTraversal[describeVars.length];

    for (int i = 0; i < describeVars.length; i++) {
      String var = describeVars[i];
      Var descVar = new Var(var);
      Var subject = new Var("subject");
      Var predicate = new Var("predicate");
      Var object = new Var("object");
      GraphTraversal<?, ?> traversal1 = __.select(var).as("subject");
      TransformOneLengthTriple.getDataAndObjectProperties(traversal1, descVar, predicate, object,
          queryData);

      GraphTraversal<?, ?> traversal2 = __.select(var).as("object");
      TransformOneLengthTriple.getReverseObjectProperties(traversal2, subject, predicate, descVar,
          queryData);
      unionTraversals[i] = __.union((Traversal) traversal1, (Traversal) traversal2);
    }
    traversal = traversal.union(unionTraversals).select("subject", "predicate", "object");

  }

  /**
   * This method adds select statement to traversal depending upon the number of variables
   *
   * @param traversal {@link GraphTraversal} object storing the current traversal
   * @param vars {@link String} array storing the variables to project
   * @return updated {@link GraphTraversal}
   */
  public GraphTraversal<?, ?> generateSelect(GraphTraversal<?, ?> traversal, String[] vars) {
    switch (vars.length) {
      case 0:
        throw new IllegalStateException();
      case 1:
        return traversal.select(vars[0], vars[0]);
      case 2:
        return traversal.select(vars[0], vars[1]);
      default:
        return traversal.select(vars[0], vars[1], Arrays.copyOfRange(vars, 2, vars.length));
    }
  }

  /**
   * This method converts the parse tree with {@link Order} operator at the root
   *
   * {@see <a href= "https://www.w3.org/TR/sparql11-query/#modOrderBy">ORDER BY</a>}
   *
   * @param tupleExpr subtree of the parse tree corresponding to the actual query with {@link Order}
   *        at the root
   * @param traversal current gremlin traversal
   * @param cur {@link Set} of variables already visited by current traversal
   */
  private void translate(Order tupleExpr, GraphTraversal<?, ?> traversal, Set<Var> cur) {
    translate(tupleExpr.getArg(), traversal, cur);

    for (OrderElem element : tupleExpr.getElements()) {
      String var = ((Var) element.getExpr()).getName();
      org.apache.tinkerpop.gremlin.process.traversal.Order orderDirection =
          element.isAscending() ? org.apache.tinkerpop.gremlin.process.traversal.Order.asc
              : org.apache.tinkerpop.gremlin.process.traversal.Order.desc;
      traversal = traversal.order().by(__.select(var), orderDirection);
    }

  }

  private void translate(SingletonSet tupleExpr, GraphTraversal<?, ?> traversal, Set<Var> cur) {
    // do nothing
  }

  /**
   * This method converts the parse tree with {@link Slice} operator at the root
   *
   * {@see <a href= "https://www.w3.org/TR/sparql11-query/#modOffset">OFFSET</a> and
   * <a href= "https://www.w3.org/TR/sparql11-query/#modLimit">LIMIT</a>}
   *
   * @param tupleExpr subtree of the parse tree corresponding to the actual query with {@link Slice}
   *        at the root
   * @param traversal current gremlin traversal
   * @param cur {@link Set} of variables already visited by current traversal
   */
  private void translate(Slice tupleExpr, GraphTraversal<?, ?> traversal, Set<Var> cur) {
    translate(tupleExpr.getArg(), traversal, cur);

    long offset = tupleExpr.getOffset() < 0 ? 0 : tupleExpr.getOffset();
    long limit = tupleExpr.getLimit() < 0 ? Long.MAX_VALUE / 2 : tupleExpr.getLimit();
    traversal = traversal.range(offset, offset + limit);
  }

  /**
   * This method converts the parse tree with {@link Projection} operator at the root
   *
   * {@see <a href= "https://www.w3.org/TR/sparql11-query/#select">SELECT</a> }
   *
   * @param tupleExpr subtree of the parse tree corresponding to the actual query with
   *        {@link Projection} at the root
   * @param traversal current gremlin traversal
   * @param cur {@link Set} of variables already visited by current traversal
   */
  private void translate(Projection tupleExpr, GraphTraversal<?, ?> traversal, Set<Var> cur) {
    List<ProjectionElem> projectionList = tupleExpr.getProjectionElemList().getElements();
    Set<String> vars =
        projectionList.stream().map(elem -> elem.getTargetName()).collect(Collectors.toSet());
    String[] selectVars = vars.toArray(new String[0]);

    queryData.addQueryVariable(vars);

    traversal = traversal.constant("N/A").as(selectVars[0],
        Arrays.copyOfRange(selectVars, 1, selectVars.length));
    translate(tupleExpr.getArg(), traversal, cur);

    for (ProjectionElem elem : projectionList) {
      // TODO: check if something else can be done to handle zero length
      // path
      if (elem.getTargetName().startsWith("_const")) {
        continue;
      }
      if (!elem.getTargetName().equals(elem.getSourceName())) {
        traversal = traversal.select(elem.getSourceName()).as(elem.getTargetName());
      }
    }
    traversal = generateSelect(traversal, selectVars);

  }

  /**
   * This method converts the parse tree with {@link MultiProjection} operator at the root Used with
   * CONSTRUCT clause
   * {@see <a href= "https://www.w3.org/TR/sparql11-query/#constructGraph">CONSTRUCT</a> }
   *
   * @param tupleExpr subtree of the parse tree corresponding to the actual query with
   *        {@link Projection} at the root
   * @param traversal current gremlin traversal
   * @param cur {@link Set} of variables already visited by current traversal
   */
  private void translate(MultiProjection tupleExpr, GraphTraversal<?, ?> traversal, Set<Var> cur) {
    GraphTraversal<Vertex, ?>[] unionTraversals =
        new GraphTraversal[tupleExpr.getProjections().size()];
    Set<String> vars = new HashSet<String>();
    for (int i = 0; i < tupleExpr.getProjections().size(); i++) {
      List<ProjectionElem> projectionList = tupleExpr.getProjections().get(i).getElements();
      GraphTraversal<Vertex, ?> tr = __.<Vertex>start();
      for (ProjectionElem elem : projectionList) {
        if (!elem.getTargetName().equals(elem.getSourceName())) {
          tr = tr.select(elem.getSourceName()).as(elem.getTargetName());
        }
        vars.add(elem.getTargetName());
      }
      unionTraversals[i] = tr;
    }
    String[] selectVars = vars.toArray(new String[0]);
    queryData.addQueryVariable(vars);

    traversal = traversal.constant("N/A").as(selectVars[0],
        Arrays.copyOfRange(selectVars, 1, selectVars.length));
    translate(tupleExpr.getArg(), traversal, cur);
    traversal = traversal.union((Traversal<?, Map>[]) unionTraversals);
    traversal = generateSelect(traversal, selectVars);
  }

  /**
   * This method converts the parse tree with {@link Extension} operator at the root
   *
   * {@see <a href= "https://www.w3.org/TR/sparql11-query/#bind">BIND</a> }
   *
   * @param tupleExpr subtree of the parse tree corresponding to the actual query with
   *        {@link Extension} at the root
   * @param traversal current gremlin traversal
   * @param cur {@link Set} of variables already visited by current traversal
   */
  private void translate(Extension tupleExpr, GraphTraversal<?, ?> traversal, Set<Var> cur) {
    // applyIndexedFilter(traversal, tupleExpr, cur);
    translate(tupleExpr.getArg(), traversal, cur);
    for (ExtensionElem elem : tupleExpr.getElements()) {
      String asVar = elem.getName();

      if (elem.getExpr() instanceof AggregateOperator) {
        String exprVar = queryData.getAlias((AggregateOperator) elem.getExpr());
        traversal = traversal.select(exprVar).as(asVar);
      } else if (elem.getExpr() instanceof ValueConstant) {
        String exprVar = ((ValueConstant) elem.getExpr()).getValue().stringValue();
        traversal = traversal.constant(exprVar).as(asVar);
      } else if (elem.getExpr() instanceof Var) {
        String exprVar = ((Var) elem.getExpr()).getName();
        traversal = traversal.select(exprVar).as(asVar);
      } else if (elem.getExpr() instanceof MathExpr) {
        String operation = generateString((MathExpr) elem.getExpr());
        traversal = traversal.math(operation).as(asVar);
      } else if (elem.getExpr() instanceof FunctionCall) {
        FunctionCall fCall = (FunctionCall) elem.getExpr();
        if (fCall.getURI().compareTo(FN.CONCAT.stringValue()) == 0) {
          List<ValueExpr> list = fCall.getArgs();
          final Function<Traverser<HashMap<String, String>>, String> function =
              (Traverser<HashMap<String, String>> tr) -> {
                String str = "";
                for (ValueExpr expr : list) {
                  if (expr instanceof Var) {
                    str += tr.get().get(((Var) expr).getName());
                  } else if (expr instanceof ValueConstant) {
                    str += ((ValueConstant) expr).getValue().stringValue();
                  }
                }

                return str;
              };
          traversal = traversal.map((Function) function).as(asVar);
        } else if (fCall.getURI().compareTo(FN.NUMERIC_FLOOR.stringValue()) == 0) {
          String var = ((Var) (fCall.getArgs().get(0))).getName();
          traversal = traversal.math("floor " + var).as(asVar);
        } else if (fCall.getURI().compareTo(FN.NUMERIC_CEIL.stringValue()) == 0) {
          String var = ((Var) (fCall.getArgs().get(0))).getName();
          traversal = traversal.math("ceil " + var).as(asVar);
        } else if (fCall.getURI().compareTo(FN.NUMERIC_ABS.stringValue()) == 0) {
          String var = ((Var) (fCall.getArgs().get(0))).getName();
          traversal = traversal.math("abs " + var).as(asVar);
        } else if (fCall.getURI().compareTo(FN.NUMERIC_ROUND.stringValue()) == 0) {
          String var = ((Var) (fCall.getArgs().get(0))).getName();
          traversal = traversal.choose(__.math(var + " - floor " + var).is(P.gte(0.5)),
              __.math("ceil " + var), __.math("floor " + var)).as(asVar);
        } else if (fCall.getURI().compareTo("RAND") == 0) {
          Double rand = Math.random();
          traversal = traversal.constant(rand).as(asVar);
        } else if (fCall.getURI().compareTo(XMLSchema.DOUBLE.stringValue()) == 0) {
          String var = ((Var) (fCall.getArgs().get(0))).getName();
          traversal = traversal.select(var).as(asVar);
        }
      }
    }

  }

  /**
   * It converts {@link MathExpr} into equivalent string representation
   *
   * @param operand {@link Value}
   * @return equivalent string representation
   */
  private String getOperandValue(ValueExpr operand) {
    String operandString = "";
    if (operand instanceof MathExpr) {
      operandString = generateString((MathExpr) operand);
    } else if (operand instanceof Var) {
      operandString = ((Var) operand).getName();
    } else if (operand instanceof ValueConstant) {
      operandString = ((ValueConstant) operand).getValue().stringValue();
    } else if (operand instanceof FunctionCall) {
      operandString = ((Var) ((FunctionCall) operand).getArgs().get(0)).getName();
    }
    return operandString;
  }

  /**
   * It converts {@link MathExpr} into equivalent infix expression
   *
   * @param expr {@link MathExpr}
   * @return {@link String} containing the infix expression
   */
  public String generateString(MathExpr expr) {
    String leftString = "", rightString = "";
    ValueExpr left = expr.getLeftArg();
    ValueExpr right = expr.getRightArg();
    return "(" + getOperandValue(left) + expr.getOperator().getSymbol() + getOperandValue(right)
        + ")";
  }

  /**
   * This method converts the parse tree with {@link Distinct} operator at the root
   *
   * {@see <a href= "https://www.w3.org/TR/sparql11-query/#modDuplicates">DISTINCT</a>}
   *
   * @param tupleExpr subtree of the parse tree corresponding to the actual query with
   *        {@link Distinct} at the root
   * @param traversal current gremlin traversal
   * @param cur {@link Set} of variables already visited by current traversal
   */
  private void translate(Distinct tupleExpr, GraphTraversal<?, ?> traversal, Set<Var> cur) {
    translate(tupleExpr.getArg(), traversal, cur);

    Set<String> vars = new HashSet<String>();
    for (String elem : tupleExpr.getBindingNames()) {
      // TODO: check if something else can be done to handle zero length
      // path
      if (!elem.startsWith("_const")) {
        vars.add(elem);
      }
    }
    String[] distinctVar = vars.toArray(new String[0]);
    traversal = traversal.dedup(distinctVar);
  }

  /**
   * This method converts the parse tree with {@link Filter} operator at the root
   *
   * {@see <a href= "https://www.w3.org/TR/sparql11-query/#restrictString">FILTER</a>}
   *
   * @param tupleExpr subtree of the parse tree corresponding to the actual query with
   *        {@link Filter} at the root
   * @param traversal current gremlin traversal
   * @param cur {@link Set} of variables already visited by current traversal
   */
  private void translate(Filter tupleExpr, GraphTraversal<?, ?> traversal, Set<Var> cur) {
    if (canApplyIndexedFilter(tupleExpr, tupleExpr.getArg(), cur)) {
      applyIndexedFilter(traversal, tupleExpr, cur);
    }
    translate(tupleExpr.getArg(), traversal, cur);
    TransformFilter.transform(tupleExpr.getCondition(), traversal, cur, queryData);
  }

  /**
   * This method converts the parse tree with {@link Group} operator at the root
   *
   * {@see <a href= "https://www.w3.org/TR/sparql11-query/#groupby">GROUP BY</a>}
   *
   * @param tupleExpr subtree of the parse tree corresponding to the actual query with {@link Group}
   *        at the root
   * @param traversal current gremlin traversal
   * @param cur {@link Set} of variables already visited by current traversal
   */
  private void translate(Group tupleExpr, GraphTraversal<?, ?> traversal, Set<Var> cur) {
    translate(tupleExpr.getArg(), traversal, cur);

    String[] selectVars =
        new String[queryData.getQueryMetaData().extractVariablesName(tupleExpr.getArg()).size()];
    queryData.getQueryMetaData().extractVariablesName(tupleExpr.getArg()).toArray(selectVars);
    traversal = generateSelect(traversal, selectVars);

    String[] groupVars = tupleExpr.getGroupBindingNames().toArray(new String[0]);

    // Step 1: Group
    traversal = traversal.group();

    // Step 2: Define key grouping
    if (groupVars.length > 0) {
      traversal = traversal.by(generateSelect(__.<Vertex>start(), groupVars));
    } else {
      traversal = traversal.by(__.constant(queryData.getAnonymousVariable()));
    }

    // Step 3: Define aggregators
    GraphTraversal<?, ?>[] aggregateTraversals =
        new GraphTraversal<?, ?>[tupleExpr.getGroupElements().size()];
    String[] asVars = new String[tupleExpr.getGroupElements().size()];
    for (int i = 0; i < tupleExpr.getGroupElements().size(); i++) {

      GroupElem aggregator = tupleExpr.getGroupElements().get(i);
      // Call aggregators
      aggregateTraversals[i] =
          TransformGroupAggregators.transform(aggregator.getOperator(), queryData);
      asVars[i] = queryData.getAlias(aggregator.getOperator());
    }
    traversal = traversal.by(generateSelect(__.fold().match(aggregateTraversals), asVars));

    // Step 4: Modify output
    traversal = traversal.unfold();
    GraphTraversal<?, ?>[] matchTraversals =
        new GraphTraversal<?, ?>[groupVars.length + asVars.length];
    for (int i = 0; i < groupVars.length; i++) {
      matchTraversals[i] = __.as("group").select(Column.keys).select(groupVars[i]).as(groupVars[i]);
    }
    for (int i = 0; i < asVars.length; i++) {
      matchTraversals[i + groupVars.length] =
          __.as("group").select(Column.values).select(asVars[i]).as(asVars[i]);
    }
    traversal = traversal.match(matchTraversals);
  }

  /**
   * It collects all the triples present in BGP into a list and other operators into separate list
   *
   * @param tupleExpr subtree of the parse tree corresponding to the actual
   *
   * @param triples {@link@ List} containing the plain triples
   * @param nonTriples {@link@ List} containing the complex operators
   */
  public void processJoin(TupleExpr tupleExpr, ArrayList<TupleExpr> triples,
      ArrayList<TupleExpr> nonTriples) {
    if (tupleExpr instanceof Join) {
      processJoin(((Join) tupleExpr).getLeftArg(), triples, nonTriples);
      processJoin(((Join) tupleExpr).getRightArg(), triples, nonTriples);
    } else if (tupleExpr instanceof BindingSetAssignment || tupleExpr instanceof Service) {
      nonTriples.add(tupleExpr);
    } else {
      triples.add(tupleExpr);
    }
  }

  /**
   * This method converts the parse tree with {@link Join} operator at the root
   *
   * @param tupleExpr subtree of the parse tree corresponding to the actual query with {@link Join}
   *        at the root
   * @param traversal current gremlin traversal
   * @param cur {@link Set} of variables already visited by current traversal
   */
  private void translate(Join tupleExpr, GraphTraversal<?, ?> traversal, Set<Var> cur) {
    ArrayList<TupleExpr> triples = new ArrayList<TupleExpr>();
    ArrayList<TupleExpr> nonTriples = new ArrayList<TupleExpr>();
    processJoin(tupleExpr, triples, nonTriples);
    translateBGP(triples, nonTriples, traversal, cur);
  }

  /**
   * This method converts the parse tree with {@link Difference} operator at the root
   *
   * {@see <a href= "https://www.w3.org/TR/sparql11-query/#neg-minus">MINUS</a>}
   *
   * @param tupleExpr subtree of the parse tree corresponding to the actual query with
   *        {@link Difference} at the root
   * @param traversal current gremlin traversal
   * @param cur {@link Set} of variables already visited by current traversal
   */
  private void translate(Difference tupleExpr, GraphTraversal<?, ?> traversal, Set<Var> cur) {
    translate(tupleExpr.getLeftArg(), traversal, cur);
    GraphTraversal right = __.<Vertex>start();
    translate(tupleExpr.getRightArg(), right, cur);
    traversal = traversal.where(__.not(right));
  }


  /**
   * It collects all the triples present in BGP into a list and other operators into separate list
   *
   * @param tupleExpr subtree of the parse tree corresponding to the actual
   *
   * @param triples {@link@ List} containing the plain triples
   * @param nonTriples {@link@ List} containing the complex operators
   */
  public void processUnion(TupleExpr tupleExpr, ArrayList<TupleExpr> triples) {
    if (tupleExpr instanceof Union) {
      processUnion(((Union) tupleExpr).getLeftArg(), triples);
      processUnion(((Union) tupleExpr).getRightArg(), triples);
    } else {
      triples.add(tupleExpr);
    }
  }

  /**
   * This method converts the parse tree with {@link Union} operator at the root
   *
   * {@see <a href= "https://www.w3.org/TR/sparql11-query/#alternatives">UNION</a>}
   *
   * @param tupleExpr subtree of the parse tree corresponding to the actual query with {@link Union}
   *        at the root
   * @param traversal current gremlin traversal
   * @param cur {@link Set} of variables already visited by current traversal
   */
  private void translate(Union tupleExpr, GraphTraversal<?, ?> traversal, Set<Var> cur) {
    ArrayList<TupleExpr> triples = new ArrayList<TupleExpr>();
    processUnion(tupleExpr, triples);
    ArrayList<Traversal> unions = new ArrayList<Traversal>();
    Set<Var> updatedCur = new HashSet<Var>();
    for (TupleExpr triple : triples) {
      Traversal tr = __.<Vertex>start();
      Set<Var> newCur = new HashSet<Var>(cur);
      translate(triple, (GraphTraversal<?, ?>) tr, newCur);
      unions.add(tr);
      updatedCur.addAll(newCur);
    }
    cur.addAll(updatedCur);
    traversal = traversal.union(unions.toArray(new Traversal[0]));
  }

  /**
   * This method converts the parse tree with {@link LeftJoin} operator at the root
   *
   * {@see <a href= "https://www.w3.org/TR/sparql11-query/#optionals">OPTIONAL</a>}
   *
   * @param tupleExpr subtree of the parse tree corresponding to the actual query with
   *        {@link LeftJoin} at the root
   * @param traversal current gremlin traversal
   * @param cur {@link Set} of variables already visited by current traversal
   */
  private void translate(LeftJoin tupleExpr, GraphTraversal<?, ?> traversal, Set<Var> cur) {
    translate(tupleExpr.getLeftArg(), traversal, cur);
    GraphTraversal match = __.<Vertex>start();
    translate(tupleExpr.getRightArg(), match, cur);

    // Handle conditions on left join
    if (tupleExpr.getCondition() != null) {
      TransformFilter.transform(tupleExpr.getCondition(), match, cur, queryData);
    }

    traversal = traversal.optional(match);
  }

  /**
   * This method converts the parse tree with {@link BindingSetAssignment} operator at the root
   *
   * {@see <a href= "https://www.w3.org/TR/sparql11-query/#inline-data">VALUES</a>}
   *
   * @param tupleExpr subtree of the parse tree corresponding to the actual query with
   *        {@link BindingSetAssignment} at the root
   * @param traversal current gremlin traversal
   * @param cur {@link Set} of variables already visited by current traversal
   */
  private void translate(BindingSetAssignment tupleExpr, GraphTraversal<?, ?> traversal,
      Set<Var> cur) {
    ArrayList<TupleExpr> nonTriples = new ArrayList<TupleExpr>();
    ArrayList<TupleExpr> triples = new ArrayList<TupleExpr>();
    nonTriples.add(tupleExpr);
    translateBGP(triples, nonTriples, traversal, cur);
  }

  /**
   * It converts {@link Service} into {@link BindingSetAssignment}
   *
   * @param tupleExpr {@link Service}
   * @return {@link BindingSetAssignment}
   */
  public BindingSetAssignment convert(Service tupleExpr) {
    // Execute the Federated Query and convert in into VALUES
    // clause/BindingSetAssignment
    Repository rep = new SPARQLRepository(tupleExpr.getServiceRef().getValue().stringValue());
    rep.initialize();
    TupleQuery query = rep.getConnection()
        .prepareTupleQuery(tupleExpr.getSelectQueryString(tupleExpr.getServiceVars()));
    TupleQueryResult res = query.evaluate();
    BindingSetAssignment bindingSetAssignment = new BindingSetAssignment();
    bindingSetAssignment.setBindingNames(new HashSet<>(res.getBindingNames()));
    bindingSetAssignment.setBindingSets(new Iterable<BindingSet>() {

      @Override
      public Iterator<BindingSet> iterator() {
        return new Iterator<BindingSet>() {

          @Override
          public boolean hasNext() {
            return res.hasNext();
          }

          @Override
          public BindingSet next() {
            return res.next();
          }
        };
      }
    });

    return bindingSetAssignment;
    // System.out.println(tupleExpr.getSelectQueryString(tupleExpr.getServiceVars()));
  }

  /**
   * This method converts the parse tree with {@link StatementPattern} operator at the root
   *
   * @param tupleExpr subtree of the parse tree corresponding to the actual query with
   *        {@link StatementPattern} at the root
   * @param traversal current gremlin traversal
   * @param cur {@link Set} of variables already visited by current traversal
   */
  private void translate(StatementPattern tupleExpr, GraphTraversal<?, ?> traversal, Set<Var> cur) {
    // If Property does not exist return empty result
    if (RDF4JHelper.isConcrete(tupleExpr.getPredicateVar()) && !queryData.getQueryMetaData()
        .isProperty(tupleExpr.getPredicateVar().getValue().stringValue())) {
      traversal = traversal.limit(0);
      return;
    }
    if (cur.contains(tupleExpr.getSubjectVar())) {
      TransformOneLengthTriple.transform(traversal, tupleExpr, false, queryData);
    } else if (cur.contains(tupleExpr.getObjectVar())) {
      TransformOneLengthTriple.transform(traversal, tupleExpr, true, queryData);
    } else {
      if (queryData.getQueryMetaData().getIndexedFilter(tupleExpr) != null) {
        applyIndexedFilter(traversal, tupleExpr, cur);
      } else {
        cur.add(tupleExpr.getSubjectVar());
      }
      translate(tupleExpr, traversal, cur);
    }
    cur.addAll(queryData.getQueryMetaData().extractVariables(tupleExpr));
  }

  /**
   * This method converts the parse tree with {@link ArbitraryLengthPath} operator at the root
   *
   * {@see <a href= "https://www.w3.org/TR/sparql11-query/#propertypaths">Property Path</a>}
   *
   * @param tupleExpr subtree of the parse tree corresponding to the actual query with
   *        {@link ArbitraryLengthPath} at the root
   * @param traversal current gremlin traversal
   * @param cur {@link Set} of variables already visited by current traversal
   */
  private void translate(ArbitraryLengthPath tupleExpr, GraphTraversal<?, ?> traversal,
      Set<Var> cur) {
    // TODO
    // if Property does not exist still it can be treated as
    // ZeroLengthTriple
    // if (RDF4JHelper.isConcrete(QueryUtils.getPredicate(tupleExpr))
    // &&
    // !propertyMapping.isProperty(QueryUtils.getPredicate(tupleExpr).getValue().stringValue()))
    // TransformZeroLengthTriple.transform(traversal, tupleExpr, false,
    // metadata);
    // else
    if (cur.contains(tupleExpr.getSubjectVar())) {
      TransformArbitraryLengthTriple.transform(traversal, tupleExpr, false, queryData);
    } else if (cur.contains(tupleExpr.getObjectVar())) {
      TransformArbitraryLengthTriple.transform(traversal, tupleExpr, true, queryData);
    } else {
      applyIndexedFilter(traversal, tupleExpr, cur);
      translate(tupleExpr, traversal, cur);
    }
    cur.addAll(queryData.getQueryMetaData().extractVariables(tupleExpr));
  }

  /**
   * This method converts the parse tree with {@link ZeroLengthPath} operator at the root
   *
   * {@see <a href= "https://www.w3.org/TR/sparql11-query/#propertypaths">Property Path</a>}
   *
   * @param tupleExpr subtree of the parse tree corresponding to the actual query with
   *        {@link ZeroLengthPath} at the root
   * @param traversal current gremlin traversal
   * @param cur {@link Set} of variables already visited by current traversal
   */
  private void translate(ZeroLengthPath tupleExpr, GraphTraversal<?, ?> traversal, Set<Var> cur) {
    if (cur.contains(tupleExpr.getSubjectVar())) {
      TransformZeroLengthTriple.transform(traversal, tupleExpr, false, queryData);
    } else if (cur.contains(tupleExpr.getObjectVar())) {
      TransformZeroLengthTriple.transform(traversal, tupleExpr, true, queryData);
    } else {
      applyIndexedFilter(traversal, tupleExpr, cur);
      translate(tupleExpr, traversal, cur);
    }
    cur.addAll(queryData.getQueryMetaData().extractVariables(tupleExpr));
  }

  /**
   * It translates list of TupleExpr(excluding SERVICE and BINDINGSETASSIGNMENT) to gremlin
   * traversal
   *
   * @param triples {@link List} containing the TupleExprs
   * @param traversal current Gremlin Traversal
   * @param cur {@link Set} of variables already visited by current traversal
   */
  private void translateBGP(List<TupleExpr> triples, GraphTraversal<?, ?> traversal, Set<Var> cur) {
    List<TupleExpr> sortedTriples =
        declarativeQueryPreprocessor.transform(triples, new HashSet<>(cur));
    for (TupleExpr tupleExpr : sortedTriples) {
      translate(tupleExpr, traversal, cur);
    }
  }

  /**
   * It translates list of TupleExpr to gremlin traversal
   *
   * @param triples {@link List} containing the TupleExprs excluding {@link BindingSetAssignment}
   *        and {@link Service}
   * @param nonTriples {@link List} containing {@link BindingSetAssignment} and {@link Service}
   *        expressions
   * @param traversal current traversal
   * @param cur {@link Set} of variables already visited by current traversal
   */
  private void translateBGP(ArrayList<TupleExpr> triples, ArrayList<TupleExpr> nonTriples,
      GraphTraversal<?, ?> traversal, Set<Var> cur) {
    // collect all bindingSetAssignments
    List<BindingSetAssignment> bindingSetAssignments = new ArrayList<BindingSetAssignment>();
    for (TupleExpr tupleExpr : nonTriples) {
      if (tupleExpr instanceof BindingSetAssignment) {
        bindingSetAssignments.add(((BindingSetAssignment) tupleExpr));
      } else if (tupleExpr instanceof Service) {
        bindingSetAssignments.add(convert((Service) tupleExpr));
      }
    }
    BindingSetAssignmentIterator bindingSetAssignmentIterator =
        new BindingSetAssignmentIterator(bindingSetAssignments);

    boolean isBinded = bindingSetAssignmentIterator.isBinded();
    if (isBinded) {
      // Apply indexes
      for (BindingSetAssignment bs : bindingSetAssignments) {
        applyIndexedFilter(traversal, bs, cur);
      }
      // apply union
      if (triples.size() > 0) {
        Iterator<Set<Var>> it = bindingSetAssignmentIterator.getBindingNamesJoin();
        ArrayList<GraphTraversal<?, ?>> traversals = new ArrayList<>();
        Set<Var> oldCur = new HashSet<Var>(cur);
        while (it.hasNext()) {
          Set<Var> newCur = it.next();
          newCur.addAll(oldCur);
          GraphTraversal<?, ?> tr = __.start();
          translateBGP(triples, tr, newCur);
          traversals.add(tr);
          cur.addAll(newCur);
        }
        traversal = traversal.union(traversals.toArray(new Traversal[0]));
      }

    } else {
      if (triples.size() > 0) {
        translateBGP(triples, traversal, cur);
      } else {
        ArrayList<GraphTraversal<?, ?>> unionTraversal = new ArrayList<>();
        Iterator<BindingSet> joinIterator =
            bindingSetAssignmentIterator.getBindingSetAssignmentJoin();
        while (joinIterator.hasNext()) {
          BindingSet bs = joinIterator.next();
          GraphTraversal<?, ?> tr = __.start();
          for (String name : bs.getBindingNames()) {
            tr.constant(RDF4JHelper.getValue(bs.getBinding(name).getValue())).as(name);
          }
          unionTraversal.add(tr);
        }
        traversal = traversal.union(unionTraversal.toArray(new Traversal[0]));
      }
      // traversal = EmptyGraph.instance().traversal().V();
    }
    for (BindingSetAssignment bs : bindingSetAssignments) {
      applyBindingSetAssignmentAsFilter(bs, traversal);
    }
  }

  /**
   * It checks whether indexed filter to given tupleExpr can be utilized
   *
   * @param tupleExpr expression
   * @param child child of expression
   * @param cur {@link Set} of variables already visited by current traversal
   * @return True if indexed filter can be used, False otherwise
   */
  private boolean canApplyIndexedFilter(TupleExpr tupleExpr, TupleExpr child, Set<Var> cur) {
    List<IndexedFilter> filters = queryData.getQueryMetaData().getIndexedFilter(tupleExpr);
    // If filter is null or all the variables used in filter are already
    // visited
    if (filters == null
        || cur.containsAll(filters.stream().map(a -> a.getAsVar()).collect(Collectors.toSet()))) {
      return false;
    }
    if (!Collections.disjoint(cur, queryData.getQueryMetaData().extractVariables(child))) {
      return false;
    }
    if (queryData.getQueryMetaData().isBetterIndexed(tupleExpr, child)) {
      return true;
    }

    return false;
  }

  /**
   * It applies IndexedFilter to the Gremlin traversal
   *
   * @param traversal Gremlin Traversal
   * @param filters indexed filters
   * @param cur {@link Set} of variables already visited by current traversal
   */
  private void applyIndexedFilter(GraphTraversal<?, ?> traversal, TupleExpr tupleExpr,
      Set<Var> cur) {
    List<IndexedFilter> filters = queryData.getQueryMetaData().getIndexedFilter(tupleExpr);
    if (filters == null) {
      return;
    }

    ArrayList<GraphTraversal<?, ?>> traversals = new ArrayList<>();
    for (IndexedFilter filter : filters) {
      GraphTraversal<?, ?> tr = __.start();
      tr = tr.V();
      if (filter.getFilter() != null) {
        tr = tr.or(filter.getFilter());
      }
      String asVar = queryData.getAnonymousVariable(filter.getAsVar());
      tr = tr.as(asVar);
      // tr = tr.has("id", TextP.containing(":"));
      tr = tr.where(__.select(Pop.first, asVar).as(asVar));
      traversals.add(tr);
    }
    GraphTraversal<?, Map<String, Object>>[] unionTraversals =
        new GraphTraversal[traversals.size()];
    for (int i = 0; i < traversals.size(); i++) {
      unionTraversals[i] = (GraphTraversal<?, Map<String, Object>>) traversals.get(i);
    }
    traversal = traversal.union(unionTraversals);
    Set<Var> asVars = filters.stream().map(a -> a.getAsVar()).collect(Collectors.toSet());

    cur.addAll(asVars);
  }

  /**
   * It converts BindingSetAssignment to Filter
   *
   * @param tupleExpr BindingSetAssignment object
   * @param traversal Gremlin traversal
   */
  private void applyBindingSetAssignmentAsFilter(BindingSetAssignment tupleExpr,
      GraphTraversal<?, ?> traversal) {
    // TransformFilter.transform(transformToFilterCondition(tupleExpr), traversal,
    // new HashSet<>());
    Iterator<BindingSet> it = tupleExpr.getBindingSets().iterator();
    ArrayList<GraphTraversal<?, ?>> orTraversals = new ArrayList<>();

    while (it.hasNext()) {
      BindingSet set = it.next();
      ArrayList<GraphTraversal<?, ?>> andTraversals = new ArrayList<>();
      for (String var : set.getBindingNames()) {
        if (set.hasBinding(var)) {
          andTraversals
              .add(__.select(var).is(P.eq(RDF4JHelper.getValue(set.getBinding(var).getValue()))));
        } else {
          andTraversals.add(__.select(var));
        }
      }
      orTraversals.add(__.and(andTraversals.toArray(new Traversal[0])));
    }
    if (orTraversals.size() > 0) {
      traversal = traversal.where(__.or(orTraversals.toArray(new Traversal[0])));
    }
  }

  /**
   * This class implements iterator to find out join of binding name present in the multiple Binding
   * Set Assignment
   *
   * @author Udit-Sharma
   *
   */
  public class BindingSetAssignmentIterator {
    List<BindingSetAssignment> bindingSetAssignments;

    public Iterator<Set<Var>> getBindingNamesJoin() {
      List<Iterator<String>> bindingNamesIterators = new ArrayList<Iterator<String>>();
      for (int i = 0; i < bindingSetAssignments.size(); i++) {
        bindingNamesIterators.add(bindingSetAssignments.get(i).getBindingNames().iterator());
      }

      return new Iterator<Set<Var>>() {
        List<Var> cur;

        @Override
        public boolean hasNext() {
          for (Iterator<String> it : bindingNamesIterators) {
            if (it.hasNext()) {
              return true;
            }
          }
          return false;
        }

        public void update() {
          if (cur == null) {
            cur = new ArrayList<Var>();
            for (int i = 0; i < bindingSetAssignments.size(); i++) {
              cur.add(new Var(bindingNamesIterators.get(i).next()));
            }
            return;
          }
          for (int i = 0; i < bindingNamesIterators.size(); i++) {
            if (bindingNamesIterators.get(i).hasNext()) {
              cur.set(i, new Var(bindingNamesIterators.get(i).next()));
              break;
            } else {
              bindingNamesIterators.set(i,
                  bindingSetAssignments.get(i).getBindingNames().iterator());
              cur.set(i, new Var(bindingNamesIterators.get(i).next()));
            }
          }
        }

        @Override
        public Set<Var> next() {
          update();
          HashSet<Var> ret = new HashSet<Var>(cur);
          return ret;

        }
      };
    }

    public Iterator<BindingSet> getBindingSetAssignmentJoin() {
      List<Iterator<BindingSet>> bindingSetIterators = new ArrayList<Iterator<BindingSet>>();
      List<BindingSet> bindings = new ArrayList<BindingSet>();
      for (BindingSetAssignment bindingSetAssignment : bindingSetAssignments) {
        bindingSetIterators.add(bindingSetAssignment.getBindingSets().iterator());
      }
      for (Iterator<BindingSet> it : bindingSetIterators) {
        bindings.add(it.next());
      }

      return new Iterator<BindingSet>() {
        BindingSet cur = null;
        {
          cur = getCartesianProduct();
          if (cur == null) {
            updateIterator();
          }
        }

        private BindingSet getCartesianProduct() {
          MapBindingSet ret = new MapBindingSet();
          for (BindingSet bs : bindings) {
            for (String name : bs.getBindingNames()) {
              if (bs.hasBinding(name)) {
                Value value = bs.getValue(name);
                if (ret.hasBinding(name) && !ret.getValue(name).equals(value)) {
                  return null;
                } else {
                  ret.addBinding(name, value);
                }
              }
            }
          }
          return ret;
        }

        private void updateIterator() {
          do {
            for (int i = 0; i < bindingSetIterators.size(); i++) {
              if (bindingSetIterators.get(i).hasNext()) {
                bindings.set(i, bindingSetIterators.get(i).next());
                break;
              } else {
                if (i == bindingSetIterators.size() - 1) {
                  cur = null;
                  return;
                }
                bindingSetIterators.set(i,
                    bindingSetAssignments.get(i).getBindingSets().iterator());
                bindings.set(i, bindingSetIterators.get(i).next());
              }

            }
          } while (getCartesianProduct() == null);
          cur = getCartesianProduct();
        }

        @Override
        public boolean hasNext() {
          return cur != null;
        }

        @Override
        public BindingSet next() {
          BindingSet ret = cur;
          updateIterator();
          return ret;
        }

      };

    }

    public boolean isBinded() {
      Iterator<Set<Var>> bs = this.getBindingNamesJoin();
      if (!bs.hasNext()) {
        return false;
      }
      Set<Var> indexedVars = this.bindingSetAssignments.stream()
          .map(a -> queryData.getQueryMetaData().getIndexedFilter(a)).filter(c -> c != null)
          .flatMap(Collection::stream).map(b -> b.getAsVar()).collect(Collectors.toSet());
      while (bs.hasNext()) {
        if (Collections.disjoint(bs.next(), indexedVars)) {
          return false;
        }
      }
      return true;
    }

    public void main(String[] args) {
      BindingSetAssignment bs1 = new BindingSetAssignment();
      bs1.setBindingNames(new HashSet<>(Arrays.asList("x1", "x2")));
      bs1.setBindingSets(Arrays.asList(
          new ListBindingSet(Arrays.asList("x1", "x2"),
              Arrays.asList(SimpleValueFactory.getInstance().createLiteral("a"),
                  SimpleValueFactory.getInstance().createLiteral("b"))),
          new ListBindingSet(Arrays.asList("x1", "x2"),
              Arrays.asList(SimpleValueFactory.getInstance().createLiteral("c"),
                  SimpleValueFactory.getInstance().createLiteral("d")))));

      BindingSetAssignment bs2 = new BindingSetAssignment();
      bs2.setBindingNames(new HashSet<>(Arrays.asList("x1", "x3")));
      bs2.setBindingSets(Arrays.asList(
          new ListBindingSet(Arrays.asList("x1", "x3"),
              Arrays.asList(SimpleValueFactory.getInstance().createLiteral("a"),
                  SimpleValueFactory.getInstance().createLiteral("x"))),
          new ListBindingSet(Arrays.asList("x1", "x3"),
              Arrays.asList(SimpleValueFactory.getInstance().createLiteral("c"),
                  SimpleValueFactory.getInstance().createLiteral("y")))));

      BindingSetAssignment bs3 = new BindingSetAssignment();
      bs3.setBindingNames(new HashSet<>(Arrays.asList("x3")));
      bs3.setBindingSets(Arrays.asList(
          new ListBindingSet(Arrays.asList("x3"),
              Arrays.asList(SimpleValueFactory.getInstance().createLiteral("z"))),
          new ListBindingSet(Arrays.asList("x3"),
              Arrays.asList(SimpleValueFactory.getInstance().createLiteral("y"))),
          new ListBindingSet(Arrays.asList("x3"),
              Arrays.asList(SimpleValueFactory.getInstance().createLiteral("x")))));

      List<BindingSetAssignment> bindingSetAssignments =
          new ArrayList<BindingSetAssignment>(Arrays.asList(bs1, bs2, bs3));
      Iterator bs = new BindingSetAssignmentIterator(bindingSetAssignments).getBindingNamesJoin();
      while (bs.hasNext()) {
        System.out.println(bs.next());
      }
    }

    public BindingSetAssignmentIterator(List<BindingSetAssignment> bindingSetAssignments) {
      this.bindingSetAssignments = bindingSetAssignments;
    }

  }

}
