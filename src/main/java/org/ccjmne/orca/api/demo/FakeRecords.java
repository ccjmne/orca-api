package org.ccjmne.orca.api.demo;

import static org.ccjmne.orca.jooq.classes.Tables.TRAININGTYPES;

import java.sql.Date;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.lang3.StringUtils;
import org.ccjmne.orca.api.utils.Constants;
import org.ccjmne.orca.jooq.classes.tables.records.EmployeesRecord;
import org.ccjmne.orca.jooq.classes.tables.records.SitesRecord;
import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.Record4;
import org.jooq.Row4;
import org.jooq.Table;
import org.jooq.impl.DSL;

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
	private final Range<LocalDate> cancelledTraining = Range.closed(LocalDate.now().minusYears(4), LocalDate.now());
	private final Range<LocalDate> completedTraining = Range.closed(LocalDate.now().minusYears(4), LocalDate.now());
	private final Range<LocalDate> scheduledTraining = Range.closed(LocalDate.now(), LocalDate.now().plusMonths(6));

	public static SitesRecord randomSite(final int primaryKey) {
		return INSTANCE.site(Integer.valueOf(primaryKey));
	}

	public static EmployeesRecord randomEmployee(final int primaryKey) {
		return INSTANCE.employee(Integer.valueOf(primaryKey));
	}

	public static Table<Record4<Integer, Date, String, String>> randomSessions(final String outcome, final int amount) {
		return INSTANCE.sessions(outcome, amount);
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

	@SuppressWarnings("unchecked")
	private Table<Record4<Integer, Date, String, String>> sessions(final String outcome, final int amount) {
		return DSL.values(IntStream.range(0, amount)
				.mapToObj(i -> {
					final Date date;
					switch (outcome) {
						case Constants.TRNG_OUTCOME_CANCELLED:
							date = FakeRecords.anyFrom(this.cancelledTraining);
							break;
						case Constants.TRNG_OUTCOME_COMPLETED:
							date = FakeRecords.anyFrom(this.completedTraining);
							break;
						case Constants.TRNG_OUTCOME_SCHEDULED:
							date = FakeRecords.anyFrom(this.scheduledTraining);
							break;
						default:
							// Can *not* happen
							throw new IllegalArgumentException(String.format("The outcome of a training must be one of %s.", Constants.TRAINING_OUTCOMES));
					}

					return DSL.row(FakeRecords.asFields(FakeRecords.random(TRAININGTYPES, TRAININGTYPES.TRTY_PK), date, outcome, ""));
				})
				.toArray(Row4[]::new));
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

	private static List<? extends Field<?>> asFields(final Object... values) {
		return Arrays.asList(values).stream().map(v -> v instanceof Field<?> ? (Field<?>) v : DSL.val(v)).collect(Collectors.toList());
	}

	private static <R> Field<R> random(final Table<?> table, final Field<R> field, final Condition... conditions) {
		return DSL.select(field).from(table).where(conditions).orderBy(DSL.rand()).limit(1).asField();
	}
}
