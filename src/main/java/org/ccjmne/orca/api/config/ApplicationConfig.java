package org.ccjmne.orca.api.config;

import javax.inject.Singleton;

import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClients;
import org.ccjmne.orca.api.demo.DemoDataManager;
import org.ccjmne.orca.api.modules.ResourcesUnrestricted;
import org.ccjmne.orca.api.modules.Restrictions;
import org.ccjmne.orca.api.utils.CustomObjectMapper;
import org.ccjmne.orca.api.utils.PostgresDSLContext;
import org.ccjmne.orca.api.utils.RestrictedResourcesAccess;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.process.internal.RequestScoped;
import org.glassfish.jersey.server.ResourceConfig;
import org.jooq.DSLContext;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ApplicationConfig extends ResourceConfig {

	public ApplicationConfig() {

		register(new AbstractBinder() {

			@Override
			protected void configure() {
				bind(HttpClients.createDefault()).to(HttpClient.class);
				bind(CustomObjectMapper.class).to(ObjectMapper.class).in(Singleton.class);
				bind(DemoDataManager.class).to(DemoDataManager.class).in(Singleton.class);
				bind(PostgresDSLContext.class).to(DSLContext.class).in(Singleton.class);
				bind(ResourcesUnrestricted.class).to(ResourcesUnrestricted.class).in(Singleton.class);
				bind(RestrictedResourcesAccess.class).to(RestrictedResourcesAccess.class).in(RequestScoped.class);
				bind(Restrictions.class).to(Restrictions.class).in(RequestScoped.class);
				bind(AmazonS3ClientBuilder.standard()
                    .withRegion(Regions.EU_WEST_1)
                    .withDualstackEnabled(Boolean.TRUE)
                    .build()).to(AmazonS3.class);
			}
		});

		register(MultiPartFeature.class);
		packages("org.ccjmne.orca.api.config.providers, org.ccjmne.orca.api.rest");
	}
}
