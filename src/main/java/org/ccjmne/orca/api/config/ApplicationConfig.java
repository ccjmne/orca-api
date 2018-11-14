package org.ccjmne.orca.api.config;

import javax.inject.Singleton;

import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClients;
import org.ccjmne.orca.api.demo.DemoDataManager;
import org.ccjmne.orca.api.modules.QueryParameters;
import org.ccjmne.orca.api.modules.RecordsCollator;
import org.ccjmne.orca.api.modules.ResourcesUnrestricted;
import org.ccjmne.orca.api.modules.Restrictions;
import org.ccjmne.orca.api.utils.CustomObjectMapper;
import org.ccjmne.orca.api.utils.PostgresDSLContext;
import org.ccjmne.orca.api.utils.ResourcesSelection;
import org.ccjmne.orca.api.utils.S3Client;
import org.ccjmne.orca.api.utils.StatisticsSelection;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.process.internal.RequestScoped;
import org.glassfish.jersey.server.ResourceConfig;
import org.jooq.DSLContext;

import com.amazonaws.services.s3.AmazonS3Client;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ApplicationConfig extends ResourceConfig {

	public ApplicationConfig() {

		super.register(new AbstractBinder() {

			@Override
			protected void configure() {
				super.bind(HttpClients.createDefault()).to(HttpClient.class);
				super.bind(CustomObjectMapper.class).to(ObjectMapper.class).in(Singleton.class);
				super.bind(DemoDataManager.class).to(DemoDataManager.class).in(Singleton.class);
				super.bind(PostgresDSLContext.class).to(DSLContext.class).in(Singleton.class);
				super.bind(QueryParameters.class).to(QueryParameters.class).in(RequestScoped.class);
				super.bind(ResourcesUnrestricted.class).to(ResourcesUnrestricted.class).in(Singleton.class);
				super.bind(ResourcesSelection.class).to(ResourcesSelection.class).in(RequestScoped.class);
				super.bind(Restrictions.class).to(Restrictions.class).in(RequestScoped.class);
				super.bind(RecordsCollator.class).to(RecordsCollator.class).in(RequestScoped.class);
				super.bind(S3Client.class).to(AmazonS3Client.class).in(Singleton.class);
				super.bind(StatisticsSelection.class).to(StatisticsSelection.class).in(RequestScoped.class);
			}
		});

		super.register(MultiPartFeature.class);
		super.packages("org.ccjmne.orca.api.config.providers, org.ccjmne.orca.api.rest");
	}
}
