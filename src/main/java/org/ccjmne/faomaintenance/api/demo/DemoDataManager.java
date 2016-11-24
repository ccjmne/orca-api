package org.ccjmne.faomaintenance.api.demo;

import java.util.Collections;
import java.util.Date;

import javax.inject.Inject;

import org.jooq.DSLContext;
import org.jooq.impl.DSL;
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

public class DemoDataManager {

	private static final String DEMO_PROPERTY = "demo";

	// Defaults to every SUNDAY at 3:00 AM
	private static final String SCHEDULE_CRON_EXPRESSION = System.getProperty("demo-cronwipe", "0 0 3 ? * SUN");
	private static final TriggerKey TRIGGER_KEY = new TriggerKey("trigger");

	private final Scheduler scheduler;
	private final DSLContext ctx;

	@Inject
	public DemoDataManager(final DSLContext ctx) throws SchedulerException {
		final DirectSchedulerFactory schedulerFactory = DirectSchedulerFactory.getInstance();
		schedulerFactory.createVolatileScheduler(1);
		this.scheduler = schedulerFactory.getScheduler();
		this.ctx = ctx;
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

	public void start() throws SchedulerException {
		if (!isDemoEnabled() || this.scheduler.isStarted()) {
			return;
		}

		final JobKey jobKey = new JobKey("reset");
		// Schedules reset job in accordance with the CRON expression
		this.scheduler.scheduleJob(
									JobBuilder
											.newJob(DemoDataReset.class)
											.usingJobData(new JobDataMap(Collections.singletonMap(DSLContext.class.getName(), this.ctx)))
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
			try (final DSLContext ctx = (DSLContext) context.getMergedJobDataMap().get(DSLContext.class.getName())) {
				LOGGER.info("Restoring demo data...");
				ctx.transaction(config -> {
					try (final DSLContext transactionCtx = DSL.using(config)) {
						DemoBareWorkingState.restore(transactionCtx);
						DemoCommonResources.generate(transactionCtx);
						DemoDataSitesEmployees.generate(transactionCtx);
						DemoDataTrainings.generate(transactionCtx);
						DemoDataUsers.generate(transactionCtx);
					} catch (final Exception e) {
						// TODO: remove
						e.printStackTrace();
						LOGGER.error("An error occured during demo data restoration.", e);
					}
				});

				LOGGER.info("Demo data restoration successfully completed.");
			} catch (final Exception e) {
				LOGGER.error("An error occured during demo data restoration.", e);
			}
		}
	}
}
