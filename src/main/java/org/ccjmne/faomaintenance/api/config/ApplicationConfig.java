package org.ccjmne.faomaintenance.api.config;

import java.text.DateFormat;

import javax.inject.Singleton;

import org.ccjmne.faomaintenance.api.db.PostgresDSLContext;
import org.ccjmne.faomaintenance.api.utils.CustomObjectMapper;
import org.ccjmne.faomaintenance.api.utils.SQLDateFormat;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.jooq.DSLContext;

import com.fasterxml.jackson.databind.ObjectMapper;

public class ApplicationConfig extends ResourceConfig {

	public ApplicationConfig() {
		register(new AbstractBinder() {

			@Override
			protected void configure() {
				bind(CustomObjectMapper.class).to(ObjectMapper.class);
				bind(SQLDateFormat.class).to(SQLDateFormat.class).to(DateFormat.class);
				bind(PostgresDSLContext.class).to(DSLContext.class).in(Singleton.class);
			}
		});

		register(MultiPartFeature.class);
		packages("org.ccjmne.faomaintenance.api.config.providers, org.ccjmne.faomaintenance.api.rest");
	}
}
