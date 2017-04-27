package org.ccjmne.orca.api.rest;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;

import org.ccjmne.orca.api.demo.DemoDataManager;
import org.quartz.SchedulerException;

@Path("demo")
@Singleton
public class DemoEndpoint {

	private static final DateFormat WITH_TIME = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss Z");

	private final DemoDataManager manager;

	@Inject
	public DemoEndpoint(final DemoDataManager manager) {
		this.manager = manager;
	}

	@GET
	public boolean isDemo() {
		return this.manager.isDemoEnabled();
	}

	@GET
	@Path("next")
	public Response getNextFireTime() throws SchedulerException {
		final Date nextFireTime = this.manager.getNextFireTime();
		if (null == nextFireTime) {
			return Response.noContent().build();
		}

		return Response.ok(WITH_TIME.format(nextFireTime)).build();
	}
}
