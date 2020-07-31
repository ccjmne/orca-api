package org.ccjmne.orca.api.config;

import java.net.http.HttpClient;

import javax.inject.Singleton;

import org.ccjmne.orca.api.demo.DemoDataManager;
import org.ccjmne.orca.api.inject.business.QueryParams;
import org.ccjmne.orca.api.inject.business.RecordsCollator;
import org.ccjmne.orca.api.inject.business.Restrictions;
import org.ccjmne.orca.api.inject.clients.PostgresDSLContext;
import org.ccjmne.orca.api.inject.core.ResourcesSelection;
import org.ccjmne.orca.api.inject.core.StatisticsSelection;
import org.ccjmne.orca.api.utils.CustomObjectMapper;
import org.ccjmne.orca.api.utils.ParamsAssertion;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.process.internal.RequestScoped;
import org.glassfish.jersey.server.ResourceConfig;
import org.jooq.DSLContext;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ApplicationConfig extends ResourceConfig {

  /**
   * Creates an instance of {@link AmazonS3Client} for a specific
   * {@link Region}.<br />
   * Expects either of these two sets of properties to be set appropriately:
   * <ul>
   * <li>Environment Variables - <code>AWS_ACCESS_KEY_ID</code> and
   * <code>AWS_SECRET_KEY</code></li>
   * <li>Java System Properties - <code>aws.accessKeyId</code> and
   * <code>aws.secretKey</code></li>
   * </ul>
   */
  private static final Regions  REGION   = Regions.EU_WEST_1;
  private static final AmazonS3 S3CLIENT = AmazonS3ClientBuilder.standard().withRegion(REGION).build();

  public ApplicationConfig() {
    super.register(new AbstractBinder() {

      @Override
      protected void configure() {
        // Clients
        super.bind(HttpClient.newHttpClient()).to(HttpClient.class);
        super.bind(S3CLIENT).to(AmazonS3.class);

        // Business modules
        super.bind(PostgresDSLContext.class).to(DSLContext.class).in(Singleton.class);
        super.bind(Restrictions.class).to(Restrictions.class).in(RequestScoped.class);
        super.bind(QueryParams.class).to(QueryParams.class).in(RequestScoped.class);
        super.bind(ParamsAssertion.class).to(ParamsAssertion.class).in(RequestScoped.class);
        super.bind(RecordsCollator.class).to(RecordsCollator.class).in(RequestScoped.class);

        // Core modules
        super.bind(ResourcesSelection.class).to(ResourcesSelection.class).in(RequestScoped.class);
        super.bind(StatisticsSelection.class).to(StatisticsSelection.class).in(RequestScoped.class);

        // Utilities
        super.bind(CustomObjectMapper.class).to(ObjectMapper.class).in(Singleton.class);

        // Demo engine
        super.bind(DemoDataManager.class).to(DemoDataManager.class).in(Singleton.class);
      }
    });

    super.register(MultiPartFeature.class);
    super.packages("org.ccjmne.orca.api.config.providers", "org.ccjmne.orca.api.rest");
  }
}
