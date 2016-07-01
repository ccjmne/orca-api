package org.ccjmne.faomaintenance.api.config;

import javax.inject.Singleton;

import org.ccjmne.faomaintenance.api.db.PostgresDSLContext;
import org.ccjmne.faomaintenance.api.modules.ResourcesUnrestricted;
import org.ccjmne.faomaintenance.api.modules.Restrictions;
import org.ccjmne.faomaintenance.api.modules.StatisticsCaches;
import org.ccjmne.faomaintenance.api.utils.CustomObjectMapper;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.process.internal.RequestScoped;
import org.glassfish.jersey.server.ResourceConfig;
import org.jooq.DSLContext;

import com.fasterxml.jackson.databind.ObjectMapper;

public class ApplicationConfig extends ResourceConfig {

	public ApplicationConfig() {
		register(new AbstractBinder() {

			@Override
			protected void configure() {
				bind(CustomObjectMapper.class).to(ObjectMapper.class).in(Singleton.class);
				bind(PostgresDSLContext.class).to(DSLContext.class).in(Singleton.class);
				bind(ResourcesUnrestricted.class).to(ResourcesUnrestricted.class).in(Singleton.class);
				bind(StatisticsCaches.class).to(StatisticsCaches.class).in(Singleton.class);
				bind(Restrictions.class).to(Restrictions.class).in(RequestScoped.class);
			}
		});

		register(MultiPartFeature.class);
		packages("org.ccjmne.faomaintenance.api.config.providers, org.ccjmne.faomaintenance.api.rest");
	}
}
