package org.ccjmne.orca.api.demo;

import java.sql.Date;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.lang3.StringUtils;
import org.ccjmne.orca.jooq.classes.tables.records.EmployeesRecord;
import org.ccjmne.orca.jooq.classes.tables.records.SitesRecord;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.collect.Range;

public class FakeRecords {

	private static final ThreadLocalRandom RANDOM = ThreadLocalRandom.current();
	private static final FakeRecords INSTANCE = new FakeRecords();

	private final Map<String, List<String>> firstNames;
	private final List<String> lastNames;
	private final List<String> cities;

	private final Range<LocalDate> dobRange = Range.closed(LocalDate.now().minusYears(60), LocalDate.now().minusYears(20));

	public static SitesRecord randomSite(final int primaryKey) {
		return INSTANCE.site(Integer.valueOf(primaryKey));
	}

	public static EmployeesRecord randomEmployee(final int primaryKey) {
		return INSTANCE.employee(Integer.valueOf(primaryKey));
	}

	@SuppressWarnings("unchecked")
	private FakeRecords() {
		try {
			final Map<String, Object> res = new ObjectMapper(new YAMLFactory())
					.readValue(	FakeRecords.class.getClassLoader().getResourceAsStream("resources.yaml"),
								TypeFactory.defaultInstance().constructParametricType(Map.class, String.class, Object.class));
			this.lastNames = (List<String>) res.get("lastNames");
			this.firstNames = (Map<String, List<String>>) res.get("firstNames");
			this.cities = (List<String>) res.get("cities");
		} catch (final Exception e) {
			// Can *not* happen
			throw new RuntimeException(e);
		}
	}

	private SitesRecord site(final Integer primaryKey) {
		final String cityName = FakeRecords.anyFrom(this.cities);
		return new SitesRecord(	primaryKey,
								cityName,
								"",
								FakeRecords.asEmail(cityName),
								String.format("site-%04d", primaryKey));
	}

	private EmployeesRecord employee(final Integer primaryKey) {
		final boolean isMale = RANDOM.nextBoolean();
		final String firstName = FakeRecords.anyFrom(this.firstNames.get(isMale ? "male" : "female"));
		final String surname = FakeRecords.anyFrom(this.lastNames);
		return new EmployeesRecord(
									primaryKey,
									firstName,
									surname,
									FakeRecords.anyFrom(this.dobRange),
									Boolean.valueOf(RANDOM.nextBoolean()),
									FakeRecords.asEmail(String.format("%s.%s", firstName, surname)),
									"",
									Boolean.valueOf(isMale),
									String.format("empl-%04d", primaryKey));
	}

	private static String asEmail(final String userName) {
		return String.format(	"%s@demo.orca-solution.com",
								StringUtils
										.stripAccents(userName)
										.replaceAll("[ ']+", "-")
										.replaceAll("[^a-zA-Z.-]", "").toLowerCase());
	}

	private static String anyFrom(final List<String> source) {
		return source.get(RANDOM.nextInt(source.size()));
	}

	private static Date anyFrom(final Range<LocalDate> dateRange) {
		return Date.valueOf(LocalDate
				.ofEpochDay(RANDOM.nextLong(dateRange.lowerEndpoint().toEpochDay(), dateRange.upperEndpoint().toEpochDay())));
	}
}
