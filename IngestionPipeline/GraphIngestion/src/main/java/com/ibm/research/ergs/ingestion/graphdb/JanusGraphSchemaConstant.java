
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
package com.ibm.research.ergs.ingestion.graphdb;

/**
 * This class contains JanusGraph internal schema related constants
 * 
 * @author Sumit Neelam (sumit.neelam@in.ibm.com)
 *
 */
public class JanusGraphSchemaConstant {
  public static String ID_PROPERTY = "id";
  public static String ORG_ID_PROPERTY = "oid";

  public static String STATNODE_INSTANCE_COUNT_KEY = "instancecount";
  public static String STATNODE_DOMAIN_COUNT_KEY = "domaincount";
  public static String STATNODE_RANGE_COUNT_KEY = "rangecount";
  public static String STATNODE_RANGE_UNIQUE_VALUES_KEY = "uniquevalues";

  public static String INFERRED_RELATION = "inferred";
  public static String EDGE_TYPE_KEY = "reltype";

  private static final String CONTEXT_PROPERTY_CONST = "context";
  private static final String SUMMARYNODE_CONTEXT_PROPERTY_VALUE_CONST = "summary";
  private static final String SUMMARYNODE_AND_DATANODE_EDGE_LABEL_CONST = "summaryOf";
  private static final String SUMMARYNODE_ID_CONST = "sid";
  private static final String SUMMARYEDGECOUNTPROPERTY_CONST = "count";
  private static final String SUMMARYCOUNTPROPERTY_CONST = "nodecount";
  private static final String SUMMARYEDGE_CONST = "connectedTO";
  private static final String SUMMARYEDGELABELPROPERTY_CONST = "edgeLabel";
  private static final String SUMMARYNODE_TYPE_CONST = "type";

  /*
   * summary specific constant
   */
  public static String CONTEXT_PROPERTY = "context";
  public static String SUMMARYNODE_CONTEXT_PROPERTY_VALUE = "summary";
  public static String SUMMARYNODE_AND_DATANODE_EDGE_LABEL = "summaryOf";
  public static String SUMMARYNODE_ID = "sid";
  public static String SUMMARYEDGECOUNTPROPERTY = "count";
  public static String SUMMARYCOUNTPROPERTY = "nodecount";
  public static String SUMMARYEDGE = "connectedTO";
  public static String SUMMARYEDGELABELPROPERTY = "edgeLabel";
  public static String SUMMARYNODE_TYPE = "type";

  public static String baseURIStr;

  public static void updateConstVariable(String baseURI) {
    baseURIStr = baseURI;
    CONTEXT_PROPERTY = baseURI + CONTEXT_PROPERTY_CONST;
    SUMMARYNODE_CONTEXT_PROPERTY_VALUE = baseURI + SUMMARYNODE_CONTEXT_PROPERTY_VALUE_CONST;
    SUMMARYNODE_AND_DATANODE_EDGE_LABEL = baseURI + SUMMARYNODE_AND_DATANODE_EDGE_LABEL_CONST;
    SUMMARYNODE_ID = baseURI + SUMMARYNODE_ID_CONST;
    SUMMARYEDGECOUNTPROPERTY = baseURI + SUMMARYEDGECOUNTPROPERTY_CONST;
    SUMMARYCOUNTPROPERTY = baseURI + SUMMARYCOUNTPROPERTY_CONST;
    SUMMARYEDGE = baseURI + SUMMARYEDGE_CONST;
    SUMMARYEDGELABELPROPERTY = baseURI + SUMMARYEDGELABELPROPERTY_CONST;
    SUMMARYNODE_TYPE = baseURI + SUMMARYNODE_TYPE_CONST;
  }
}
