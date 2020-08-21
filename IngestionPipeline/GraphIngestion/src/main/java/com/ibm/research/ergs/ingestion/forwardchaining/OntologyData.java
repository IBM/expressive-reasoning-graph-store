
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
package com.ibm.research.ergs.ingestion.forwardchaining;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.ibm.research.ergs.ingestion.graphdb.JanusGraphConnection;
import com.ibm.research.ergs.ingestion.graphdb.SchemaCreator;

/**
 * This class contains data of input tbox ontology
 * 
 * @author Sumit Neelam (sumit.neelam@in.ibm.com)
 *
 */
public class OntologyData {

  private static final Logger logger = LoggerFactory.getLogger(OntologyData.class);

  private HashMap<String, HashSet<String>> objSuperProperties;
  private HashMap<String, HashSet<String>> dataSuperProperties;
  private HashMap<String, ArrayList<String>> superClasses;
  private HashMap<String, ArrayList<String>> equivalentClasses;
  private HashMap<String, String> propertyDomain;
  private HashMap<String, String> propertyRange;
  private Set<String> transitiveProperties;
  private Set<String> symmetricProperties;
  private HashMap<String, HashSet<String>> inverseProperties;

  /**
   * gets super properties of property
   * 
   * @param propName
   * @return
   */
  public HashSet<String> getObjectSuperProperty(String propName) {
    return (objSuperProperties.get(propName));
  }

  /**
   * sets super properties of property
   * 
   * @param objSuperProperties
   */
  public void setObjectSuperProperties(HashMap<String, HashSet<String>> objSuperProperties) {
    this.objSuperProperties = objSuperProperties;
  }

  public HashMap<String, HashSet<String>> getObjectSuperProperties() {
    return (objSuperProperties);
  }

  public HashSet<String> getDataSuperProperty(String propName) {
    return (dataSuperProperties.get(propName));
  }

  public void setDataSuperProperties(HashMap<String, HashSet<String>> dataSuperProperties) {
    this.dataSuperProperties = dataSuperProperties;
  }

  public HashMap<String, HashSet<String>> getDataSuperProperties() {
    return (dataSuperProperties);
  }

  public HashMap<String, ArrayList<String>> getClassHierarchy() {
    return (superClasses);
  }

  public void setSuperClasses(HashMap<String, ArrayList<String>> superClasses) {
    this.superClasses = superClasses;
  }

  public HashMap<String, ArrayList<String>> getEquivalentClasses() {
    return (equivalentClasses);
  }

  public void setEquivalentClasses(HashMap<String, ArrayList<String>> equivalentClasses) {
    this.equivalentClasses = equivalentClasses;
  }

  public String getPropertyDomain(String propName) {
    return (propertyDomain.get(propName));
  }

  public void setPropertyDomain(HashMap<String, String> propertyDomain) {
    this.propertyDomain = propertyDomain;
  }

  public HashMap<String, String> getPropertyDomains() {
    return (propertyDomain);
  }

  public String getPropertyRange(String propName) {
    return (propertyRange.get(propName));
  }

  public void setPropertyRange(HashMap<String, String> propertyRange) {
    this.propertyRange = propertyRange;
  }

  public HashMap<String, String> getPropertyRanges() {
    return (propertyRange);
  }

  public boolean isSymmetric(String propName) {
    return (symmetricProperties.contains(propName));
  }

  public void setSymmetricProperties(Set<String> symmetricProperties) {
    this.symmetricProperties = symmetricProperties;
  }

  public Set<String> getSymmetricProperties() {
    return (symmetricProperties);
  }

  public boolean isInverse(String propName) {
    return (inverseProperties.containsKey(propName));
  }

  public HashSet<String> getInverseProperty(String propName) {
    return (inverseProperties.get(propName));
  }

  public void setInverseProperties(HashMap<String, HashSet<String>> inverseProperties) {
    this.inverseProperties = inverseProperties;
  }

  public HashMap<String, HashSet<String>> getInverseProperties() {
    return (inverseProperties);
  }

  public boolean isTransitive(String propName) {
    return (transitiveProperties.contains(propName));
  }

  public void setTransitiveProperties(Set<String> transitiveProperties) {
    this.transitiveProperties = transitiveProperties;
  }

  public Set<String> getTransitiveProperties() {
    return (transitiveProperties);
  }

  public ArrayList<String> getPropertyTypes(String prop) {
    ArrayList<String> types = new ArrayList<String>();
    if (symmetricProperties.contains(prop)) {
      types.add("symmetric");
    }
    if (inverseProperties.containsKey(prop)) {
      types.add("inverse");
    }
    if (transitiveProperties.contains(prop)) {
      types.add("transitive");
    }
    if (objSuperProperties.containsKey(prop)) {
      types.add("superproperties");
    }
    return (types);
  }

  /**
   * following function will be needed for version 2 release
   * 
   * @param janusGraphConnection
   */
  public void writeSuperPropertiesSchemaInformation(JanusGraphConnection janusGraphConnection) {
    HashMap<String, String> propertyKeys = new HashMap<String, String>();
    HashSet<String> edgeLabels = new HashSet<String>();

    SchemaCreator schemaCreator = janusGraphConnection.getSchemaCreator();
    for (String property : dataSuperProperties.keySet()) {
      HashSet<String> superProperties = dataSuperProperties.get(property);
      for (String superProperty : superProperties) {
        propertyKeys.put(superProperty, "string");
        schemaCreator.createProperty(superProperty, "string", true, true);
      }
    }

    for (String property : objSuperProperties.keySet()) {
      HashSet<String> superProperties = objSuperProperties.get(property);
      for (String superProperty : superProperties) {
        edgeLabels.add(superProperty);
        schemaCreator.createProperty(superProperty, "string", true, true);
      }
    }
    schemaCreator.commit();
  }

  public void printAllData() {
    logger.info("obj super props");
    for (String prop : objSuperProperties.keySet()) {
      HashSet<String> v = objSuperProperties.get(prop);
      if (v.size() > 0) {
        logger.info(prop + ": " + v);
      }
    }
    logger.info("data super props");
    for (String prop : dataSuperProperties.keySet()) {
      HashSet<String> v = dataSuperProperties.get(prop);
      if (v.size() > 0) {
        logger.info(prop + ": " + v);
      }
    }
    logger.info("symm prop:");
    logger.info(symmetricProperties.toString());
    logger.info("inverse props");
    logger.info(inverseProperties.toString());
    logger.info("trans props");
    logger.info(transitiveProperties.toString());
  }
}

