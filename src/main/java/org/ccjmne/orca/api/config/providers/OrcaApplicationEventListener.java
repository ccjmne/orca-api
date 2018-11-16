package org.ccjmne.orca.api.config.providers;

import javax.inject.Inject;
import javax.ws.rs.ext.Provider;

import org.ccjmne.orca.api.demo.DemoDataManager;
import org.glassfish.jersey.server.monitoring.ApplicationEvent;
import org.glassfish.jersey.server.monitoring.ApplicationEventListener;
import org.glassfish.jersey.server.monitoring.RequestEvent;
import org.glassfish.jersey.server.monitoring.RequestEventListener;
import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Provider
public class OrcaApplicationEventListener implements ApplicationEventListener {

  private static final Logger LOGGER = LoggerFactory.getLogger(OrcaApplicationEventListener.class);

  private final DemoDataManager demoDataManager;

  @Inject
  public OrcaApplicationEventListener(final DemoDataManager demoDataManager) {
    this.demoDataManager = demoDataManager;
  }

  @Override
  public void onEvent(final ApplicationEvent event) {
    switch (event.getType()) {
      case DESTROY_FINISHED:
        try {
          this.demoDataManager.shutdown();
        } catch (final SchedulerException e) {
          LOGGER.error("An error occured during scheduler shutdown.", e);
        }

        break;
      case INITIALIZATION_FINISHED:
        try {
          this.demoDataManager.start();
        } catch (final SchedulerException e) {
          LOGGER.error("An error occured during scheduler startup.", e);
        }

        break;
      // $CASES-OMITTED$
      default:
        break;
    }
  }

  @Override
  public RequestEventListener onRequest(final RequestEvent requestEvent) {
    // Don't listen to request_events
    return null;
  }
}
