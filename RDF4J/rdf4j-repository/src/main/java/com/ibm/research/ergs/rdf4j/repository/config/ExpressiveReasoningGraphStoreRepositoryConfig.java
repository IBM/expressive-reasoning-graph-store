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
package com.ibm.research.ergs.rdf4j.repository.config;

import static org.eclipse.rdf4j.repository.config.RepositoryConfigSchema.REPOSITORYID;
import static org.eclipse.rdf4j.repository.config.RepositoryConfigSchema.REPOSITORYTYPE;
import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.util.Models;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.config.AbstractRepositoryImplConfig;
import org.eclipse.rdf4j.repository.config.RepositoryConfigException;
import org.eclipse.rdf4j.repository.config.RepositoryImplConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.ibm.research.ergs.rdf4j.repository.ExpressiveReasoningGraphStoreRepository;

/**
 * {@link RepositoryImplConfig} for ERGS
 *
 * @author Udit Sharma
 *
 */
public class ExpressiveReasoningGraphStoreRepositoryConfig extends AbstractRepositoryImplConfig {
  private static final Logger logger =
      LoggerFactory.getLogger(ExpressiveReasoningGraphStoreRepositoryConfig.class);

  private ExpressiveReasoningGraphStoreRepository repository;
  private String name;
  private boolean buildSummary;
  private String tbox;
  private String config;

  public static final String NAMESPACE = "http://com.ibm.research.ergs#";
  public final static IRI BUILDSUMMARY;
  public final static IRI TBOX;
  public final static IRI CONFIG;

  static {
    SimpleValueFactory factory = SimpleValueFactory.getInstance();
    BUILDSUMMARY = factory.createIRI(NAMESPACE, "buildSummary");
    TBOX = factory.createIRI(NAMESPACE, "tbox");
    CONFIG = factory.createIRI(NAMESPACE, "config");
  }

  /**
   * create a new {@link ExpressiveReasoningGraphStoreRepositoryConfig}
   */
  public ExpressiveReasoningGraphStoreRepositoryConfig() {
    super(ExpressiveReasoningGraphStoreRepositoryFactory.REPOSITORY_TYPE);
    repository = null;
  }

  public void setBuildSummary(boolean buildSummary) {
    this.buildSummary = buildSummary;
  }

  public void setTbox(String tbox) {
    this.tbox = tbox;
  }

  public void setConfig(String config) {
    this.config = config;
  }

  public void setName(String name) {
    this.name = name;
  }

  @Override
  public void validate() throws RepositoryConfigException {
    buildRepository();

  }

  /**
   * Checks that the fields are not missing
   *
   * @throws RepositoryConfigException if name of graph is not specified
   */
  private void validateFields() throws RepositoryConfigException {
    try {
      if (name == null || name.length() == 0) {
        throw new RepositoryConfigException(
            String.format("No properties file specified for repository creation "));
      }
    } catch (SecurityException e) {
      throw new RepositoryConfigException(e.getMessage());
    }
  }

  /**
   * Creates {@link ExpressiveReasoningGraphStoreRepository}
   *
   * @return ERGS {@link Repository}
   * @throws RepositoryConfigException if config is invalid
   */
  public ExpressiveReasoningGraphStoreRepository buildRepository()
      throws RepositoryConfigException {
    /*
     * Cache (computed only once)
     */
    if (repository != null) {
      return repository;
    }

    /*
     * Common validation. May throw a RepositoryConfigException
     */
    validateFields();
    repository = new ExpressiveReasoningGraphStoreRepository(name,
        new ByteArrayInputStream(tbox.getBytes(StandardCharsets.UTF_8)),
        new ByteArrayInputStream(config.getBytes(StandardCharsets.UTF_8)), true);
    return repository;

  }

  @Override
  public Resource export(Model model) {
    Resource implNode = super.export(model);
    ValueFactory vf = SimpleValueFactory.getInstance();

    // if (name != null) {
    // graph.add(implNode, REPO_ID, vf.createLiteral(name));
    // }

    model.add(implNode, BUILDSUMMARY, vf.createLiteral(buildSummary));
    try {
      model.add(implNode, TBOX, vf.createLiteral(URLEncoder.encode(tbox, "UTF-8")));
      model.add(implNode, CONFIG, vf.createLiteral(URLEncoder.encode(config, "UTF-8")));

    } catch (UnsupportedEncodingException e) {
      logger.error(e.getMessage(), e);
    }
    return implNode;
  }

  @Override
  public void parse(Model model, Resource resource) throws RepositoryConfigException {
    super.parse(model, resource);
    try {
      // use repositoryID as name
      Models.objectLiteral(model.filter(null, REPOSITORYID, null))
          .ifPresent(lit -> this.setName(lit.getLabel()));

      Models.objectLiteral(model.filter(resource, REPOSITORYTYPE, null))
          .ifPresent(lit -> setType(lit.getLabel()));

      Models.objectLiteral(model.filter(resource, BUILDSUMMARY, null))
          .ifPresent(lit -> this.buildSummary = lit.booleanValue());

      Models.objectLiteral(model.filter(resource, TBOX, null)).ifPresent(lit -> {
        try {
          setTbox(URLDecoder.decode(lit.getLabel(), "UTF-8"));
        } catch (UnsupportedEncodingException e) {
          throw new RepositoryConfigException(e.getMessage(), e);
        }
      });
      Models.objectLiteral(model.filter(resource, CONFIG, null)).ifPresent(lit -> {
        try {
          setConfig(URLDecoder.decode(lit.getLabel(), "UTF-8"));
        } catch (UnsupportedEncodingException e) {
          throw new RepositoryConfigException(e.getMessage(), e);
        }
      });

    } catch (Exception e) {
      // System.out.println("Exception");
      throw new RepositoryConfigException(e.getMessage(), e);
    }

  }

}
