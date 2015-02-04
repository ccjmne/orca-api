package org.ccjmne.faomaintenance.api.utils;

import java.sql.DriverManager;
import java.sql.SQLException;

import org.jooq.SQLDialect;
import org.jooq.conf.Settings;
import org.jooq.impl.DefaultDSLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	static {
		try {
			Class.forName(DB_DRIVER);
		} catch (final ClassNotFoundException e) {
			LOGGER.error("Could not load DB driver: {}", DB_DRIVER);
		}
	}

	public PostgresDSLContext() throws SQLException {
		super(DriverManager.getConnection(DB_URL, DB_USER, DB_PASS), SQLDialect.POSTGRES, new ConfiguredSettings());
	}

	private static class ConfiguredSettings extends Settings {

		public ConfiguredSettings() {
			setExecuteLogging(DEBUG);
		}
	}
}