
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

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Stream;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLDataProperty;
import org.semanticweb.owlapi.model.OWLDataPropertyDomainAxiom;
import org.semanticweb.owlapi.model.OWLDataPropertyRangeAxiom;
import org.semanticweb.owlapi.model.OWLObjectProperty;
import org.semanticweb.owlapi.model.OWLObjectPropertyDomainAxiom;
import org.semanticweb.owlapi.model.OWLObjectPropertyExpression;
import org.semanticweb.owlapi.model.OWLObjectPropertyRangeAxiom;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.semanticweb.owlapi.model.OWLSymmetricObjectPropertyAxiom;
import org.semanticweb.owlapi.model.OWLTransitiveObjectPropertyAxiom;
import org.semanticweb.owlapi.reasoner.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.manchester.cs.jfact.JFactFactory;
import uk.ac.manchester.cs.jfact.JFactReasoner;
import uk.ac.manchester.cs.jfact.kernel.options.JFactReasonerConfiguration;

/**
 * This class contains functions to parse ontology file using JFact reasoner.
 * 
 * @author Sumit Neelam (sumit.neelam@in.ibm.com)
 *
 */
public class JFactOntologyProcessor {

  private static final Logger logger = LoggerFactory.getLogger(JFactOntologyProcessor.class);

  /**
   * Constructs {@link JFactOntologyProcessor}
   * 
   * @param stream
   * @return
   * @throws OWLOntologyCreationException
   */
  public OntologyData processOntology(InputStream stream) throws OWLOntologyCreationException {
    OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
    OWLOntology ont = manager.loadOntologyFromOntologyDocument(stream);
    return processOntology(ont);
  }

  /**
   * Constructs {@link JFactOntologyProcessor}
   * 
   * @param filePath
   * @return
   * @throws OWLOntologyCreationException
   */
  public OntologyData processOntology(String filePath) throws OWLOntologyCreationException {
    OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
    OWLOntology ont = manager.loadOntologyFromOntologyDocument(new File(filePath));
    return processOntology(ont);
  }

  /**
   * processes input ontology
   * 
   * @param ont
   * @return
   */
  OntologyData processOntology(OWLOntology ont) {
    try {
      Set<String> symmetricProperties = new HashSet<String>();
      Set<String> transitiveProperties = new HashSet<String>();
      HashMap<String, HashSet<String>> inverseProperties = new HashMap<String, HashSet<String>>();

      HashMap<String, String> propertyDomain = new HashMap<>();
      HashMap<String, String> propertyRange = new HashMap<>();
      HashMap<String, HashSet<String>> objSuperProperties = new HashMap<>();
      HashMap<String, HashSet<String>> dataSuperProperties = new HashMap<>();
      HashMap<String, ArrayList<String>> superClasses = new HashMap<>();
      HashMap<String, ArrayList<String>> equivalentClasses = new HashMap<>();

      transitiveProperties.add("http://www.w3.org/2000/01/rdf-schema#subPropertyOf");
      transitiveProperties.add("http://www.w3.org/2000/01/rdf-schema#subClassOf");

      JFactFactory factory = new JFactFactory();
      JFactReasonerConfiguration jrc = new JFactReasonerConfiguration();
      jrc.setAbsorptionLoggingActive(true);
      JFactReasoner reasoner = (JFactReasoner) factory.createReasoner(ont, jrc);

      Iterator<OWLObjectProperty> itr1 = ont.objectPropertiesInSignature().iterator();
      while (itr1.hasNext()) {
        OWLObjectProperty objProp = itr1.next();
        Stream<OWLSymmetricObjectPropertyAxiom> s1 = ont.symmetricObjectPropertyAxioms(objProp);
        Stream<OWLTransitiveObjectPropertyAxiom> s3 = ont.transitiveObjectPropertyAxioms(objProp);

        if (s1.count() > 0) {
          symmetricProperties.add(objProp.getIRI().getIRIString());
        }

        Iterator<OWLObjectPropertyExpression> i =
            reasoner.getInverseObjectProperties(objProp).entities().iterator();
        HashSet<String> invProps = new HashSet<String>();
        while (i.hasNext()) {
          OWLObjectPropertyExpression e = i.next();
          if (e instanceof OWLObjectProperty) {
            OWLObjectProperty prop = (OWLObjectProperty) e;
            invProps.add(prop.getNamedProperty().getIRI().getIRIString());
          }
        }
        if (invProps.size() > 0) {
          inverseProperties.put(objProp.getIRI().getIRIString(), invProps);
        }

        if (s3.count() > 0) {
          transitiveProperties.add(objProp.getIRI().getIRIString());
        }
        Iterator<OWLObjectPropertyDomainAxiom> i2 =
            ont.objectPropertyDomainAxioms(objProp).iterator();
        if (i2.hasNext()) {
          OWLObjectPropertyDomainAxiom domainAxiom = i2.next();
          Iterator<OWLClass> domItr = domainAxiom.classesInSignature().iterator();
          ArrayList<OWLClass> domainClasses = new ArrayList<>();
          while (domItr.hasNext()) {
            domainClasses.add(domItr.next());
          }
          if (domainClasses.size() == 1) {
            propertyDomain.put(objProp.getIRI().getIRIString(),
                domainClasses.get(0).getIRI().getIRIString());
          }
        }
        Iterator<OWLObjectPropertyRangeAxiom> i3 =
            ont.objectPropertyRangeAxioms(objProp).iterator();
        if (i3.hasNext()) {
          OWLObjectPropertyRangeAxiom rangeAxiom = i3.next();
          Iterator<OWLClass> rangeItr = rangeAxiom.classesInSignature().iterator();
          ArrayList<OWLClass> rangeClasses = new ArrayList<>();
          while (rangeItr.hasNext()) {
            rangeClasses.add(rangeItr.next());
          }
          if (rangeClasses.size() == 1) {
            propertyRange.put(objProp.getIRI().getIRIString(),
                rangeClasses.get(0).getIRI().getIRIString());
          }
        }

        Iterator<Node<OWLObjectPropertyExpression>> i4 =
            reasoner.getSuperObjectProperties(objProp, false).iterator();
        HashSet<String> superProps = new HashSet<String>();
        while (i4.hasNext()) {
          Node<OWLObjectPropertyExpression> node = i4.next();
          if (!node.isTopNode()) {
            Iterator<OWLObjectPropertyExpression> nodeItr = node.entities().iterator();
            while (nodeItr.hasNext()) {
              OWLObjectPropertyExpression e = nodeItr.next();
              if (e instanceof OWLObjectProperty) {
                OWLObjectProperty prop = (OWLObjectProperty) e;
                superProps.add(prop.getNamedProperty().getIRI().getIRIString());
              }
            }
          }
        }

        if (superProps.size() > 0) {
          objSuperProperties.put(objProp.getIRI().getIRIString(), superProps);
        }
      }
      Iterator<OWLDataProperty> itr2 = ont.dataPropertiesInSignature().iterator();
      while (itr2.hasNext()) {
        OWLDataProperty dataProp = itr2.next();

        Iterator<OWLDataPropertyDomainAxiom> i1 = ont.dataPropertyDomainAxioms(dataProp).iterator();
        if (i1.hasNext()) {
          OWLDataPropertyDomainAxiom domainAxiom = i1.next();
          Iterator<OWLClass> domItr = domainAxiom.classesInSignature().iterator();
          ArrayList<OWLClass> domainClasses = new ArrayList<>();
          while (domItr.hasNext()) {
            domainClasses.add(domItr.next());
          }
          if (domainClasses.size() == 1) {
            propertyDomain.put(dataProp.getIRI().getIRIString(),
                domainClasses.get(0).getIRI().getIRIString());
          }
        }
        Iterator<OWLDataPropertyRangeAxiom> i2 = ont.dataPropertyRangeAxioms(dataProp).iterator();
        if (i2.hasNext()) {
          OWLDataPropertyRangeAxiom rangeAxiom = i2.next();
          propertyRange.put(dataProp.getIRI().getIRIString(), rangeAxiom.toString());
        }

        Iterator<Node<OWLDataProperty>> i3 =
            reasoner.getSuperDataProperties(dataProp, false).iterator();
        HashSet<String> superProps = new HashSet<String>();
        while (i3.hasNext()) {
          Node<OWLDataProperty> node = i3.next();
          if (!node.isTopNode()) {
            Iterator<OWLDataProperty> nodeItr = node.entities().iterator();
            while (nodeItr.hasNext()) {
              OWLDataProperty e = nodeItr.next();
              superProps.add(e.getIRI().getIRIString());
            }
          }
        }

        if (superProps.size() > 0) {
          dataSuperProperties.put(dataProp.toString(), superProps);
        }
      }
      Iterator<OWLClass> itr3 = ont.classesInSignature().iterator();
      while (itr3.hasNext()) {
        OWLClass owlClass = itr3.next();
        Iterator<OWLClass> i1 = reasoner.superClasses(owlClass, false).iterator();
        ArrayList<String> supClasses = new ArrayList<String>();
        while (i1.hasNext()) {
          OWLClass c = i1.next();
          supClasses.add(c.getIRI().getIRIString());
        }
        superClasses.put(owlClass.getIRI().getIRIString(), supClasses);

        Iterator<OWLClass> i2 = reasoner.equivalentClasses(owlClass).iterator();
        ArrayList<String> eqvClasses = new ArrayList<String>();
        while (i2.hasNext()) {
          OWLClass c = i2.next();
          eqvClasses.add(c.getIRI().getIRIString());
        }
        equivalentClasses.put(owlClass.getIRI().getIRIString(), eqvClasses);
      }

      OntologyData ontologyData = new OntologyData();
      ontologyData.setSymmetricProperties(symmetricProperties);
      ontologyData.setTransitiveProperties(transitiveProperties);
      ontologyData.setInverseProperties(inverseProperties);
      ontologyData.setPropertyDomain(propertyDomain);
      ontologyData.setPropertyRange(propertyRange);
      ontologyData.setObjectSuperProperties(objSuperProperties);
      ontologyData.setDataSuperProperties(dataSuperProperties);
      ontologyData.setSuperClasses(superClasses);
      ontologyData.setEquivalentClasses(equivalentClasses);
      return (ontologyData);
    } catch (Exception e) {
      logger.error("Exception: ", e);
    }
    return (null);
  }
}
