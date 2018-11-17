package org.ccjmne.orca.api.demo;

import java.util.Date;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.ccjmne.orca.api.utils.Transactions;
import org.jooq.DSLContext;
import org.quartz.CronScheduleBuilder;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.quartz.impl.DirectSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.AmazonS3Client;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;

@Singleton
public class DemoDataManager {

  private static final String DEMO_PROPERTY = "demo";

  // Defaults to every SUNDAY at 3:00 AM
  private static final String SCHEDULE_CRON_EXPRESSION = System.getProperty("demo-cronwipe", "0 0 3 ? * *");

  private static final TriggerKey TRIGGER_KEY = new TriggerKey("trigger");
  private static final JobKey     JOB_KEY     = new JobKey("reset");

  // TODO: replace the entire scheduling thing w/ an *actual* cron job
  private final Scheduler scheduler;

  private final DSLContext     ctx;
  private final AmazonS3Client client;
  private final ObjectMapper   mapper;

  @Inject
  public DemoDataManager(final DSLContext ctx, final AmazonS3Client client, final ObjectMapper mapper) throws SchedulerException {
    final DirectSchedulerFactory schedulerFactory = DirectSchedulerFactory.getInstance();
    schedulerFactory.createVolatileScheduler(1);
    this.scheduler = schedulerFactory.getScheduler();
    this.ctx = ctx;
    this.client = client;
    this.mapper = mapper;
  }

  public boolean isDemoEnabled() {
    return Boolean.getBoolean(DEMO_PROPERTY);
  }

  public Date getNextFireTime() throws SchedulerException {
    if (!this.scheduler.isStarted()) {
      return null;
    }

    return this.scheduler.getTrigger(TRIGGER_KEY).getNextFireTime();
  }

  public void trigger() throws SchedulerException {
    if (!this.isDemoEnabled()) {
      return;
    }

    this.scheduler.triggerJob(JOB_KEY);
  }

  public void start() throws SchedulerException {
    if (!this.isDemoEnabled() || this.scheduler.isStarted()) {
      return;
    }

    final JobKey jobKey = JOB_KEY;
    // Schedules reset job in accordance with the CRON expression
    this.scheduler.scheduleJob(
                               JobBuilder
                                   .newJob(DemoDataReset.class)
                                   .usingJobData(new JobDataMap(ImmutableMap
                                       .<String, Object> of(DSLContext.class.getName(), this.ctx,
                                                            AmazonS3Client.class.getName(), this.client,
                                                            ObjectMapper.class.getName(), this.mapper)))
                                   .withIdentity(jobKey)
                                   .build(),
                               TriggerBuilder
                                   .newTrigger()
                                   .withSchedule(CronScheduleBuilder.cronSchedule(SCHEDULE_CRON_EXPRESSION))
                                   .withIdentity(TRIGGER_KEY)
                                   .build());
    this.scheduler.triggerJob(jobKey);
    this.scheduler.start();
  }

  public void shutdown() throws SchedulerException {
    if (this.scheduler.isStarted()) {
      this.scheduler.shutdown(true);
    }
  }

  public static class DemoDataReset implements Job {

    private static final Logger LOGGER = LoggerFactory.getLogger(DemoDataReset.class);

    @Override
    public void execute(final JobExecutionContext context) throws JobExecutionException {
      final AmazonS3Client client = (AmazonS3Client) context.getMergedJobDataMap().get(AmazonS3Client.class.getName());
      final ObjectMapper mapper = (ObjectMapper) context.getMergedJobDataMap().get(ObjectMapper.class.getName());
      try {
        LOGGER.info("Restoring demo data...");
        Transactions.with((DSLContext) context.getMergedJobDataMap().get(DSLContext.class.getName()), transactionCtx -> {
          DemoBareWorkingState.restore(transactionCtx, client);
          DemoCommonResources.generate(transactionCtx, mapper);
          DemoDataSitesEmployees.generate(transactionCtx);
          DemoDataTrainings.generate(transactionCtx);
          DemoDataUsers.generate(transactionCtx);
          LOGGER.info("Demo data restoration successfully completed.");
        });
      } catch (final Exception e) {
        LOGGER.error("An error occured during demo data restoration.", e);
        e.printStackTrace();
      }
    }
  }
}
