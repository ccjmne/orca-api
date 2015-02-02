package org.ccjmne.faomaintenance.api.jooq;

import org.jooq.util.GenerationTool;
import org.jooq.util.jaxb.Configuration;
import org.jooq.util.jaxb.Database;
import org.jooq.util.jaxb.Generator;
import org.jooq.util.jaxb.Jdbc;
import org.jooq.util.jaxb.Target;
import org.junit.Test;

public class TestGenerateJooqClasses {

	private static final String DB_DRIVER = "org.postgresql.Driver";

	private static final String DB_USER = System.getProperty("db_user", "postgres");
	private static final String DB_PASS = System.getProperty("db_pass", "asdf123");
	private static final String DB_URL = String.format(
	                                                   "jdbc:postgresql://%s:%s/%s",
	                                                   System.getProperty("db_host", "localhost"),
	                                                   System.getProperty("db_port", "5432"),
	                                                   System.getProperty("db_name", "postgres"));

	@Test
	public void generateJooqClasses() throws Exception {
		GenerationTool.generate(new Configuration().withJdbc(
		                                                     new Jdbc().withDriver(DB_DRIVER).withUrl(DB_URL).withUser(DB_USER)
		                                                     .withPassword(DB_PASS))
		                                                     .withGenerator(
		                                                                    new Generator()
		                                                                    .withName("org.jooq.util.DefaultGenerator")
		                                                                    .withDatabase(
		                                                                                  new Database().withName("org.jooq.util.postgres.PostgresDatabase").withIncludes(".*").withExcludes("")
		                                                                                  .withInputSchema("public"))
		                                                                                  .withTarget(new Target().withPackageName("org.ccjmne.faomaintenance.api.jooq").withDirectory("src/main/java"))));
	}
}
