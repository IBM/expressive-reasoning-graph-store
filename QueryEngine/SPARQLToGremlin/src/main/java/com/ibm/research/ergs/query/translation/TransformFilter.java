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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.vocabulary.FN;
import org.eclipse.rdf4j.query.algebra.And;
import org.eclipse.rdf4j.query.algebra.Bound;
import org.eclipse.rdf4j.query.algebra.Compare;
import org.eclipse.rdf4j.query.algebra.Exists;
import org.eclipse.rdf4j.query.algebra.FunctionCall;
import org.eclipse.rdf4j.query.algebra.Lang;
import org.eclipse.rdf4j.query.algebra.LangMatches;
import org.eclipse.rdf4j.query.algebra.ListMemberOperator;
import org.eclipse.rdf4j.query.algebra.MathExpr;
import org.eclipse.rdf4j.query.algebra.Not;
import org.eclipse.rdf4j.query.algebra.Or;
import org.eclipse.rdf4j.query.algebra.Regex;
import org.eclipse.rdf4j.query.algebra.SameTerm;
import org.eclipse.rdf4j.query.algebra.Str;
import org.eclipse.rdf4j.query.algebra.ValueConstant;
import org.eclipse.rdf4j.query.algebra.ValueExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.janusgraph.core.attribute.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.ibm.research.ergs.query.metadata.PropertyTypeMetaData.PropertyType;
import com.ibm.research.ergs.query.utils.RDF4JHelper;

/**
 * This class is responsible to translating Filter expression in SPARQL query to equivalent gremlin
 * traversals
 *
 * @author Udit Sharma
 *
 */
public class TransformFilter {
  private static final Logger logger = LoggerFactory.getLogger(TransformFilter.class);


  /**
   * This method uses reflection to invoke appropriate method depending upon the class of ValueExpr
   *
   * @param condition filter expression
   * @param traversal current Gremlin traversal
   * @param cur {@link Set} of variables already visited by current traversal
   * @param queryData data specific to given query
   */
  public static void transform(ValueExpr condition, GraphTraversal<?, ?> traversal, Set<Var> cur,
      QueryData queryData) {
    if (condition == null) {
      return;
    }
    Class compare = TransformFilter.class;
    try {
      Method methodcall = compare.getDeclaredMethod("transform", condition.getClass(),
          GraphTraversal.class, Set.class, QueryData.class);
      methodcall.invoke(null, condition, traversal, cur, queryData);
    } catch (NoSuchMethodException | SecurityException | IllegalAccessException
        | IllegalArgumentException | InvocationTargetException e) {
      logger.error(e.getMessage(), e);
    }

  }

  /**
   * It converts given regular expression into case insensitive regular expression
   *
   * @param regex regular expression
   * @return case insensitive regular expression
   */
  public static String getCaseInsensitiveRegex(String regex) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < regex.length(); i++) {
      char c = regex.charAt(i);
      if (Character.isLetter(c)) {
        sb.append('[').append(Character.toLowerCase(c)).append(Character.toUpperCase(c))
            .append(']');
      } else {
        sb.append(c);
      }
    }
    return sb.toString();
  }

  /**
   * This method converts {@link LangMatches} filter expression
   * {@see <a href= "https://www.w3.org/TR/sparql11-query/#func-langMatches">langMatches</a>}
   *
   * @param cond {@link LangMatches} filter expression
   * @param traversal current gremlin traversal
   * @param cur {@link Set} of variables already visited by current traversal
   * @param queryData data specific to given query
   */
  private static void transform(LangMatches cond, GraphTraversal<?, ?> traversal, Set<Var> cur,
      QueryData queryData) {
    Lang var = (Lang) cond.getLeftArg();
    Object lang = RDF4JHelper.getValue(((ValueConstant) cond.getRightArg()).getValue());

    String varName = queryData.getAnonymousVariable((Var) var.getArg());
    traversal = traversal.select(varName).values("language").where(__.is(P.eq(lang)));
  }

  /**
   * This method converts {@link Bound} filter expression
   * {@see <a href= "https://www.w3.org/TR/sparql11-query/#func-bound">BOUND</a>}
   *
   * @param cond {@link Bound} filter expression
   * @param traversal current gremlin traversal
   * @param cur {@link Set} of variables already visited by current traversal
   * @param queryData data specific to given query
   */
  private static void transform(Bound cond, GraphTraversal<?, ?> traversal, Set<Var> cur,
      QueryData queryData) {
    Var var = cond.getArg();
    traversal = traversal.select(var.getName()).where(__.is(P.neq("N/A")));
  }

  /**
   * This method converts {@link Regex} filter expression
   * {@see <a href= "https://www.w3.org/TR/sparql11-query/#func-regex">REGEX</a>}
   *
   * @param cond {@link Regex} filter expression
   * @param traversal current gremlin traversal
   * @param cur {@link Set} of variables already visited by current traversal
   * @param queryData data specific to given query
   */
  private static void transform(Regex regex, GraphTraversal<?, ?> traversal, Set<Var> cur,
      QueryData queryData) {
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
        pattern = getCaseInsensitiveRegex(pattern);
      }
    }
    String newPattern = ".*" + pattern + ".*";
    traversal = traversal.where(__.select(var.getName()).is(Text.textRegex(newPattern)));
  }

  /**
   * This method converts {@link FunctionCall} filter expression
   * {@see <a href= "hhttps://www.w3.org/TR/xpath-functions/">XPath and XQuery Functions</a>}
   *
   * @param cond {@link FunctionCall} filter expression
   * @param traversal current gremlin traversal
   * @param cur {@link Set} of variables already visited by current traversal
   * @param queryData data specific to given query
   */
  private static void transform(FunctionCall function, GraphTraversal<?, ?> traversal, Set<Var> cur,
      QueryData queryData) {
    String var = "";
    List<String> values = new ArrayList<String>();
    for (ValueExpr expr : function.getArgs()) {
      if (expr instanceof Str) {
        expr = ((Str) expr).getArg();
      }

      if (expr instanceof Var) {
        var = ((Var) expr).getName();
      } else {
        Value value = ((ValueConstant) expr).getValue();
        values.add(value.stringValue());
      }
    }
    assert values.size() == 1;
    if (function.getURI().equals(FN.CONTAINS.stringValue())) {
      traversal = traversal.where(__.select(var).is(Text.textRegex(".*" + values.get(0) + ".*")));
    } else if (function.getURI().equals(FN.ENDS_WITH.stringValue())) {
      traversal = traversal.where(__.select(var).is(Text.textRegex(values.get(0) + ".*")));
    } else if (function.getURI().equals(FN.STARTS_WITH.stringValue())) {
      traversal = traversal.where(__.select(var).is(Text.textRegex(".*" + values.get(0))));
    }

  }

  /**
   * It adds internal suffixes to property IRIs
   *
   * @param constant property IRI
   * @param addDataPropertySuffix True if data property suffix is required, False otherwise
   * @param queryData data specific to given query
   * @return string with appropriate suffixes added
   */
  private static Object addSuffix(Object constant, boolean addDataPropertySuffix,
      QueryData queryData) {
    if (constant instanceof String) {
      if (addDataPropertySuffix && (queryData.getQueryMetaData()
          .extractPropertyType(constant.toString()) == PropertyType.CONFLICTING_PROPERTY
          || queryData.getQueryMetaData()
              .extractPropertyType(constant.toString()) == PropertyType.DATATYPE_PROPERTY)) {
        return constant.toString() + "_prop";
      } else if (!addDataPropertySuffix && (queryData.getQueryMetaData()
          .extractPropertyType(constant.toString()) == PropertyType.CONFLICTING_PROPERTY
          || queryData.getQueryMetaData()
              .extractPropertyType(constant.toString()) == PropertyType.OBJECT_PROPERTY)) {
        return constant.toString() + "_edge";
      }
    }
    return constant;
  }

  /**
   * This method converts {@link SameTerm} filter expression
   * {@see <a href= "https://www.w3.org/TR/sparql11-query/#func-sameTerm">sameTerm</a>}
   *
   * @param cond {@link SameTerm} filter expression
   * @param traversal current gremlin traversal
   * @param cur {@link Set} of variables already visited by current traversal
   * @param queryData data specific to given query
   */
  private static void transform(SameTerm comparator, GraphTraversal<?, ?> traversal, Set<Var> cur,
      QueryData queryData) {
    String leftVar = ((Var) comparator.getLeftArg()).getName();
    String rightVar = ((Var) comparator.getRightArg()).getName();
    traversal = traversal.select(leftVar).where(P.eq(rightVar));
  }

  /**
   * This method converts {@link Compare} filter expression
   * {@see <a href= "https://www.w3.org/TR/sparql11-query/#restrictNumber">Compare</a>}
   *
   * @param cond {@link Compare} filter expression
   * @param traversal current gremlin traversal
   * @param cur {@link Set} of variables already visited by current traversal
   * @param queryData data specific to given query
   */
  public static void transform(Compare comparator, GraphTraversal<?, ?> traversal, Set<Var> cur,
      QueryData queryData) {
    ValueExpr left = comparator.getLeftArg();
    ValueExpr right = comparator.getRightArg();
    if (left instanceof ValueConstant && right instanceof Var) {
      String var = ((Var) right).getName();
      Object constant = RDF4JHelper.getValue(((ValueConstant) left).getValue());
      List<Object> listWithSuffix = Arrays.asList(addSuffix(constant, true, queryData),
          addSuffix(constant, false, queryData));
      switch (comparator.getOperator()) {
        case LT:
          traversal = traversal.select(var).where(__.is(P.gt(constant)));
          break;
        case LE:
          traversal = traversal.select(var).where(__.is(P.gte(constant)));
          break;
        case GE:
          traversal = traversal.select(var).where(__.is(P.lte(constant)));
          break;
        case GT:
          traversal = traversal.select(var).where(__.is(P.lt(constant)));
          break;
        case EQ:
          traversal = traversal.select(var).where(__.is(P.within(listWithSuffix)));
          break;
        case NE:
          traversal = traversal.select(var).where(__.is(P.without(listWithSuffix)));
          break;
      }
    } else if (left instanceof Var && right instanceof ValueConstant) {
      String var = ((Var) left).getName();
      Object constant = RDF4JHelper.getValue(((ValueConstant) right).getValue());
      List<Object> listWithSuffix = Arrays.asList(addSuffix(constant, true, queryData),
          addSuffix(constant, false, queryData));
      switch (comparator.getOperator()) {
        case LT:
          traversal = traversal.select(var).where(__.is(P.lt(constant)));
          break;
        case LE:
          traversal = traversal.select(var).where(__.is(P.lte(constant)));
          break;
        case GE:
          traversal = traversal.select(var).where(__.is(P.gte(constant)));
          break;
        case GT:
          traversal = traversal.select(var).where(__.is(P.gt(constant)));
          break;
        case EQ:
          traversal = traversal.select(var).where(__.is(P.within(listWithSuffix)));
          break;
        case NE:
          traversal = traversal.select(var).where(__.is(P.without(listWithSuffix)));
          break;
      }
    } else if (left instanceof Var && right instanceof Var) {
      String leftVar = ((Var) left).getName();
      String rightVar = ((Var) right).getName();
      switch (comparator.getOperator()) {
        case LT:
          traversal = traversal.select(leftVar).where(P.lt(rightVar));
          break;
        case LE:
          traversal = traversal.select(leftVar).where(P.lte(rightVar));
          break;
        case GE:
          traversal = traversal.select(leftVar).where(P.gte(rightVar));
          break;
        case GT:
          traversal = traversal.select(leftVar).where(P.gt(rightVar));
          break;
        case EQ:
          traversal = traversal.select(leftVar).where(P.eq(rightVar));
          break;
        case NE:
          traversal = traversal.select(leftVar).where(P.neq(rightVar));
          break;
      }
    } else if (left instanceof ValueConstant && right instanceof ValueConstant) {
      Object leftConstant = RDF4JHelper.getValue(((ValueConstant) left).getValue());
      Object rightConstant = RDF4JHelper.getValue(((ValueConstant) right).getValue());
      switch (comparator.getOperator()) {
        case LT:
          traversal = traversal.constant(leftConstant).where(__.is(P.lt(rightConstant)));
          break;
        case LE:
          traversal = traversal.constant(leftConstant).where(__.is(P.lte(rightConstant)));
          break;
        case GE:
          traversal = traversal.constant(leftConstant).where(__.is(P.gte(rightConstant)));
          break;
        case GT:
          traversal = traversal.constant(leftConstant).where(__.is(P.gt(rightConstant)));
          break;
        case EQ:
          traversal = traversal.constant(leftConstant).where(__.is(P.eq(rightConstant)));
          break;
        case NE:
          traversal = traversal.constant(leftConstant).where(__.is(P.neq(rightConstant)));
          break;
      }
    } else if (left instanceof Var && right instanceof MathExpr) {
      String var = ((Var) left).getName();
      String mathOperation = queryData.getQueryConvertor().generateString((MathExpr) right);
      switch (comparator.getOperator()) {
        case LT:
          traversal = traversal.math(mathOperation).where(P.gt(var));
          break;
        case LE:
          traversal = traversal.math(mathOperation).where(P.gte(var));
          break;
        case GE:
          traversal = traversal.math(mathOperation).where(P.lte(var));
          break;
        case GT:
          traversal = traversal.math(mathOperation).where(P.lt(var));
          break;
        case EQ:
          traversal = traversal.math(mathOperation).where(P.eq(var));
          break;
        case NE:
          traversal = traversal.math(mathOperation).where(P.neq(var));
          break;
      }
    } else if (left instanceof MathExpr && right instanceof Var) {
      String var = ((Var) right).getName();
      String mathOperation = queryData.getQueryConvertor().generateString((MathExpr) left);
      switch (comparator.getOperator()) {
        case LT:
          traversal = traversal.math(mathOperation).where(P.lt(var));
          break;
        case LE:
          traversal = traversal.math(mathOperation).where(P.lte(var));
          break;
        case GE:
          traversal = traversal.math(mathOperation).where(P.gte(var));
          break;
        case GT:
          traversal = traversal.math(mathOperation).where(P.gt(var));
          break;
        case EQ:
          traversal = traversal.math(mathOperation).where(P.eq(var));
          break;
        case NE:
          traversal = traversal.math(mathOperation).where(P.neq(var));
          break;
      }
    }
  }

  /**
   * This method converts {@link ListMemberOperator} filter expression
   * {@see <a href= "https://www.w3.org/TR/sparql11-query/#func-in">IN</a>}
   *
   * @param cond {@link ListMemberOperator} filter expression
   * @param traversal current gremlin traversal
   * @param cur {@link Set} of variables already visited by current traversal
   * @param queryData data specific to given query
   */
  public static void transform(ListMemberOperator operator, GraphTraversal<?, ?> traversal,
      Set<Var> cur, QueryData queryData) {
    String var = "";
    List<Object> values = new ArrayList<Object>();
    for (ValueExpr expr : operator.getArguments()) {
      if (expr instanceof Var) {
        var = ((Var) expr).getName();
      } else {
        Value value = ((ValueConstant) expr).getValue();
        values.add(RDF4JHelper.getValue(value));
      }
    }
    traversal = traversal.where(__.select(var).is(P.within(values)));
  }

  /**
   * This method converts {@link Or} filter expression
   * {@see <a href= "https://www.w3.org/TR/sparql11-query/#func-logical-or">OR</a>}
   *
   * @param cond {@link Or} filter expression
   * @param traversal current gremlin traversal
   * @param cur {@link Set} of variables already visited by current traversal
   * @param queryData data specific to given query
   */
  private static void transform(Or operator, GraphTraversal<?, ?> traversal, Set<Var> cur,
      QueryData queryData) {
    GraphTraversal<?, ?> t1 = __.start();
    GraphTraversal<?, ?> t2 = __.start();
    transform(operator.getLeftArg(), t1, cur, queryData);
    transform(operator.getRightArg(), t2, cur, queryData);
    traversal = traversal.or(t1, t2);
  }

  /**
   * This method converts {@link And} filter expression
   * {@see <a href= "https://www.w3.org/TR/sparql11-query/#func-logical-and">AND</a>}
   *
   * @param cond {@link And} filter expression
   * @param traversal current gremlin traversal
   * @param cur {@link Set} of variables already visited by current traversal
   * @param queryData data specific to given query
   */
  private static void transform(And operator, GraphTraversal<?, ?> traversal, Set<Var> cur,
      QueryData queryData) {
    GraphTraversal<?, ?> t1 = __.start();
    GraphTraversal<?, ?> t2 = __.start();
    transform(operator.getLeftArg(), t1, cur, queryData);
    transform(operator.getRightArg(), t2, cur, queryData);
    traversal = traversal.and(t1, t2);
  }

  /**
   * This method converts {@link Not} filter expression
   *
   * @param cond {@link Not} filter expression
   * @param traversal current gremlin traversal
   * @param cur {@link Set} of variables already visited by current traversal
   * @param queryData data specific to given query
   */
  private static void transform(Not operator, GraphTraversal<?, ?> traversal, Set<Var> cur,
      QueryData queryData) {
    GraphTraversal<?, ?> t = __.start();
    transform(operator.getArg(), t, cur, queryData);
    traversal = traversal.not(t);
  }

  /**
   * This method converts {@link Exists} filter expression
   * {@see <a href= "hhttps://www.w3.org/TR/sparql11-query/#func-filter-exists">EXISTS</a>}
   *
   * @param cond {@link Exists} filter expression
   * @param traversal current gremlin traversal
   * @param cur {@link Set} of variables already visited by current traversal
   * @param queryData data specific to given query
   */
  private static void transform(Exists operator, GraphTraversal<?, ?> traversal, Set<Var> cur,
      QueryData queryData) {
    GraphTraversal<?, ?> exists = __.start();
    queryData.getQueryConvertor().translate(operator.getSubQuery(), exists, cur);

    traversal = traversal.where(exists);
  }
}
