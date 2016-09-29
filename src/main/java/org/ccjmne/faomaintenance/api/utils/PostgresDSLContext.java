package org.ccjmne.faomaintenance.api.utils;

import org.jooq.SQLDialect;
import org.jooq.conf.Settings;
import org.jooq.impl.DefaultDSLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

@SuppressWarnings("serial")
public class PostgresDSLContext extends DefaultDSLContext {

	protected static final Boolean DEBUG = Boolean.valueOf(System.getProperty("debug", "FALSE"));

	private static final Logger LOGGER = LoggerFactory.getLogger(PostgresDSLContext.class);
	private static final String DB_DRIVER = "org.postgresql.Driver";

	private static final String DB_USER = System.getProperty("db_user", "postgres");
	private static final String DB_PASS = System.getProperty("db_pass", "asdf123");
	private static final String DB_URL = String.format(
														"jdbc:postgresql://%s:%s/%s",
														System.getProperty("db_host", "localhost"),
														System.getProperty("db_port", "5432"),
														System.getProperty("db_name", "postgres"));

	private static final CustomHikariSettings HIKARI_SETTINGS = new CustomHikariSettings();

	private static HikariDataSource DATA_SOURCE;
	static {
		try {
			Class.forName(DB_DRIVER);
		} catch (final ClassNotFoundException e) {
			LOGGER.error("Could not load DB driver: {}", DB_DRIVER);
		}

		final HikariConfig config = new HikariConfig();
		config.setJdbcUrl(DB_URL);
		config.setUsername(DB_USER);
		config.setPassword(DB_PASS);
		config.setMaximumPoolSize(16);
		config.addDataSourceProperty("cachePrepStmts", "true");
		config.addDataSourceProperty("prepStmtCacheSize", "250");
		config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
		config.addDataSourceProperty("useServerPrepStmts", "true");
		DATA_SOURCE = new HikariDataSource(config);
	}

	public PostgresDSLContext() {
		super(DATA_SOURCE, SQLDialect.POSTGRES, HIKARI_SETTINGS);
	}

	private static class CustomHikariSettings extends Settings {

		public CustomHikariSettings() {
			setExecuteLogging(DEBUG);
		}
	}
}