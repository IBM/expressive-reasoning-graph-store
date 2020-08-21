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
package com.ibm.research.ergs.query.utils;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.SimpleIRI;
import org.eclipse.rdf4j.model.impl.SimpleLiteral;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.eclipse.rdf4j.query.algebra.Var;

/**
 * Helper for dealing with different RDF4J specific constructs
 *
 * @author Udit Sharma
 *
 */
public class RDF4JHelper {

  /**
   *
   * @param var {@link Var} object
   * @return True if var is a variable False otherwise
   */
  public static boolean isIRI(Var var) {
    return (!isNull(var) && var.isConstant() && var.getValue() instanceof IRI);
  }

  /**
   *
   * @param var {@link Var} object
   * @return True if var is a literal False otherwise
   */
  public static boolean isLiteral(Var var) {
    return (!isNull(var) && var.isConstant() && var.getValue() instanceof Literal);
  }

  /**
   *
   * @param var {@link Var} object
   * @return True if var is a concrete False otherwise
   */
  public static boolean isConcrete(Var var) {
    return (!isNull(var) && var.isConstant());
  }

  /**
   *
   * @param var {@link Var} object
   * @return True if var is a variable False otherwise
   */
  public static boolean isVariable(Var var) {
    return (!isNull(var) && !var.isConstant());
  }

  /**
   *
   * @param var {@link Var} object
   * @return True if var is null False otherwise
   */
  public static boolean isNull(Var var) {
    return var == null;
  }

  /**
   *
   * @param literal {@link SimpleLiteral} object
   * @return value of literal depending upon the data type
   */
  public static Object getValue(SimpleLiteral literal) {
    if (literal.getDatatype().equals(XMLSchema.BOOLEAN)) {
      return literal.booleanValue();
    } else if (literal.getDatatype().equals(XMLSchema.BYTE)) {
      return literal.byteValue();
    } else if (literal.getDatatype().equals(XMLSchema.DECIMAL)) {
      return literal.decimalValue();
    } else if (literal.getDatatype().equals(XMLSchema.DOUBLE)) {
      return literal.doubleValue();
    } else if (literal.getDatatype().equals(XMLSchema.FLOAT)) {
      return literal.floatValue();
    } else if (literal.getDatatype().equals(XMLSchema.INTEGER)) {
      return literal.integerValue();
    } else if (literal.getDatatype().equals(XMLSchema.INT)) {
      return literal.intValue();
    } else if (literal.getDatatype().equals(XMLSchema.LONG)) {
      return literal.longValue();
    } else if (literal.getDatatype().equals(XMLSchema.INT)) {
      return literal.intValue();
    } else if (literal.getDatatype().equals(XMLSchema.SHORT)) {
      return literal.shortValue();
    } else if (literal.getDatatype().equals(XMLSchema.STRING)) {
      return literal.stringValue();
    } else if (literal.getDatatype().equals(RDF.LANGSTRING)) {
      return literal.getLabel();
    } else {
      return literal.getLabel();
    }
  }

  /**
   *
   * @param iri {@link SimpleIRI} object
   * @return string representation for given IRI
   */
  public static Object getValue(SimpleIRI iri) {
    return iri.stringValue();
  }

  /**
   *
   * @param value {@link Value} object
   * @return value of {@link Value} object depending upon the data type
   */
  public static Object getValue(Value value) {
    if (value instanceof SimpleIRI) {
      return getValue((SimpleIRI) value);
    } else if (value instanceof SimpleLiteral) {
      return getValue((SimpleLiteral) value);
    }
    return null;
  }

  /**
   *
   * @param name name of variable
   * @param value value of variable
   * @return create a new variable
   */
  public static Var createVar(String name, Value value) {
    Var var;
    if (value == null) {
      var = new Var(name);
    } else {
      var = new Var(name, value);
      var.setConstant(true);
    }
    return var;
  }

  /**
   *
   * @param predicate predicate IRI
   * @param isDataTypeProperty True if predicate is data type property false otherwise
   * @return predicate IRI with suffixes appended
   */
  public static String getPredicatValue(Var predicate, boolean isDataTypeProperty) {
    String ret = predicate.getValue().stringValue();
    if (isDataTypeProperty) {
      return ret + "_prop";
    } else {
      return ret + "_edge";
    }
  }

}
