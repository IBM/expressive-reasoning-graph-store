
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
package com.ibm.research.ergs.ingestion.loader;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFHandler;
import com.ibm.research.ergs.ingestion.graphdb.JanusGraphConnection;

/**
 * This is custom rdf reader listener class.
 * 
 * @author Sumit Neelam (sumit.neelam@in.ibm.com)
 *
 */
class RDFReaderListener extends AbstractRDFHandler {
  JanusGraphConnection janusGraphConnection;

  private TriplesBatch triplesBatch;
  private Set<String> rdfNamespaces;
  private boolean isFullABox;
  private String prevTBoxSubject;
  private String prevABoxSubject;
  private ExecutorService pool;

  /**
   * Constructs {@link RDFReaderListener}
   * 
   * @param janusGraphConnection
   */
  public RDFReaderListener(JanusGraphConnection janusGraphConnection) {
    this.janusGraphConnection = janusGraphConnection;

    if (InputParameters.NUM_THREADS > 1) {
      pool = Executors.newFixedThreadPool(InputParameters.NUM_THREADS, new MyThreadFactory());
      triplesBatch = new TriplesBatch(janusGraphConnection, pool);
    } else {
      triplesBatch = new TriplesBatch(janusGraphConnection, null);
    }
    this.isFullABox = InputParameters.IS_FULL_ABOX;
    rdfNamespaces = new HashSet<String>();
  }

  @Override
  public void handleNamespace(String prefix, String uri) {
    if (uri.contains("http://www.w3.org")) {
      rdfNamespaces.add(uri);
    }
  }

  @Override
  public void handleStatement(Statement st) {
    if (isFullABox || isABoxAxiom(st)) {
      triplesBatch.addTripleToBuffer(st);
    }
  }

  /**
   * Checks whether statement is tbox or abox axiom
   * 
   * @param st
   * @return
   */
  private boolean isABoxAxiom(Statement st) {
    Resource subject = st.getSubject();

    if (subject instanceof IRI) {
      if (prevTBoxSubject != null && prevTBoxSubject.equals(subject.toString())) {
        return (false);
      }
      if (prevABoxSubject != null && prevABoxSubject.equals(subject.toString())) {
        return (true);
      }
      IRI predicate = st.getPredicate();
      String predicateLocalName = predicate.getLocalName();
      String predicateNamespace = predicate.getNamespace();

      if (predicateLocalName.equalsIgnoreCase("type")
          || predicateLocalName.equalsIgnoreCase("sameAs")
          || predicateLocalName.equalsIgnoreCase("differentFrom")) {
        Value object = st.getObject();
        if (object instanceof IRI) {
          IRI i = (IRI) object;
          if (!rdfNamespaces.contains(i.getNamespace())) {
            prevABoxSubject = subject.toString();
            return (true);
          }
        }
      } else if (!rdfNamespaces.contains(predicateNamespace)) {
        prevABoxSubject = subject.toString();
        return (true);
      }
    }
    prevTBoxSubject = subject.toString();
    return (false);
  }

  /**
   * This function is called in the last of data load. It writes remaining data and performs final
   * operations.
   */
  public void finalCommit() {
    triplesBatch.writeTriplesBatch();

    rdfNamespaces.clear();
    if (InputParameters.BUILD_ALLPROP_INDEX) {
      janusGraphConnection
          .buildAllPropertyExternalIndex(InputParameters.EXTERNAL_INDEX_PREFIX + "AllProp");
    }

    if (InputParameters.NUM_THREADS > 1) {
      pool.shutdown();
      try {
        pool.awaitTermination(1440, TimeUnit.MINUTES);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Thread class for parallel load
   * 
   * @author Sumit Neelam (sumit.neelam@in.ibm.com)
   *
   */
  private class MyThreadFactory implements ThreadFactory {
    private int counter = 0;

    @Override
    public Thread newThread(Runnable r) {
      return new Thread(r, "" + counter++);
    }
  }
}
