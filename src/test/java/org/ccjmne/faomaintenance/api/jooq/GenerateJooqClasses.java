package org.ccjmne.faomaintenance.api.jooq;
import org.jooq.util.GenerationTool;
import org.jooq.util.jaxb.Configuration;
import org.jooq.util.jaxb.Database;
import org.jooq.util.jaxb.Generator;
import org.jooq.util.jaxb.Jdbc;
import org.jooq.util.jaxb.Target;

public class GenerateJooqClasses {

	public static void main(final String args[]) throws Exception {
		GenerationTool.generate(new Configuration()
				.withJdbc(new Jdbc()
						.withDriver("org.postgresql.Driver")
						.withUrl("jdbc:postgresql:postgres")
						.withUser("postgres")
						.withPassword("asdf123"))
				.withGenerator(new Generator()
						.withName("org.jooq.util.DefaultGenerator")
						.withDatabase(new Database()
								.withName("org.jooq.util.postgres.PostgresDatabase")
								.withIncludes(".*")
								.withExcludes("")
								.withInputSchema("public"))
						.withTarget(new Target()
								.withPackageName("org.ccjmne.faomaintenance.api.jooq")
								.withDirectory("src/main/java"))));
	}
}
