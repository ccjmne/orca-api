package org.ccjmne.orca.api.rest.pub;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;

import org.ccjmne.orca.api.demo.DemoDataManager;
import org.quartz.SchedulerException;

@Path("demo")
@Singleton
public class DemoEndpoint {

  private static final String SECRET = System.getProperty("init.secret");
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

  @POST
  @Path("trigger")
  public void trigger(final String secret) throws SchedulerException {
    if (!this.manager.isDemoEnabled()) {
      throw new IllegalStateException("Demo mode is not enabled.");
    }

    if ((SECRET == null) || SECRET.isEmpty() || !SECRET.equals(secret)) {
      throw new IllegalStateException("The instance is not set up for (re)initilisation or your password is invalid.");
    }

    this.manager.trigger();
  }
}
