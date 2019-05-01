package org.ccjmne.orca.api.demo;

import static org.ccjmne.orca.jooq.codegen.Tables.TRAININGTYPES;

import java.sql.Date;
import java.text.Normalizer;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.ccjmne.orca.api.utils.Constants;
import org.ccjmne.orca.jooq.codegen.tables.Employees;
import org.ccjmne.orca.jooq.codegen.tables.Sites;
import org.ccjmne.orca.jooq.codegen.tables.records.EmployeesRecord;
import org.ccjmne.orca.jooq.codegen.tables.records.SitesRecord;
import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.Record4;
import org.jooq.Row4;
import org.jooq.Table;
import org.jooq.TableRecord;
import org.jooq.impl.DSL;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.collect.Range;

@SuppressWarnings("unchecked")
public class FakeRecords {

  private static final ThreadLocalRandom RANDOM = ThreadLocalRandom.current();

  private static Map<String, List<String>> FIRST_NAMES;
  private static List<String>              LAST_NAMES;
  private static List<String>              CITIES;

  static {
    try {
      final Map<String, Object> res = new ObjectMapper(new YAMLFactory())
          .readValue(FakeRecords.class.getClassLoader().getResourceAsStream("resources.yaml"),
                     TypeFactory.defaultInstance().constructParametricType(Map.class, String.class, Object.class));
      FakeRecords.LAST_NAMES = (List<String>) res.get("lastNames");
      FakeRecords.FIRST_NAMES = (Map<String, List<String>>) res.get("firstNames");
      FakeRecords.CITIES = (List<String>) res.get("cities");
    } catch (final Exception e) {
      // Can *not* happen
      throw new RuntimeException(e);
    }
  }

  private final Range<LocalDate> dobRange;
  private final Range<LocalDate> cancelledTraining;
  private final Range<LocalDate> completedTraining;
  private final Range<LocalDate> scheduledTraining;

  public static <R> Field<R> random(final Table<?> table, final Field<R> field, final Condition... conditions) {
    return DSL.select(field).from(table).where(conditions).orderBy(DSL.rand()).limit(1).asField();
  }

  public static List<? extends Field<?>> asFields(final Object... values) {
    return Arrays.stream(values).map(v -> null == v ? DSL.field("?", v) : v instanceof Field<?> ? (Field<?>) v : DSL.val(v)).collect(Collectors.toList());
  }

  public FakeRecords() {
    this.dobRange = Range.closed(LocalDate.now().minusYears(60), LocalDate.now().minusYears(20));
    this.cancelledTraining = Range.closed(LocalDate.now().minusYears(4), LocalDate.now());
    this.completedTraining = Range.closed(LocalDate.now().minusYears(4), LocalDate.now());
    this.scheduledTraining = Range.closed(LocalDate.now(), LocalDate.now().plusMonths(6));
  }

  /**
   * Generates a random {@link SitesRecord} with no defined
   * {@link Sites#SITE_PK}.<br />
   * You'll have to either set or {@link TableRecord#reset(Field)} it before
   * persisting to the database.
   *
   * @param uniqueId
   *          A unique integer value to be used for populating this
   *          {@link SitesRecord}'s {@link Sites#SITE_EXTERNAL_ID} field.
   * @return a {@link SitesRecord} with <strong>NO</strong>
   *         {@link Sites#SITE_PK} defined.
   */
  public SitesRecord site(final Integer uniqueId) {
    final String cityName = FakeRecords.anyFrom(FakeRecords.CITIES);
    return new SitesRecord(null, cityName, "", FakeRecords.asEmail(cityName), String.format("S%04d", uniqueId));
  }

  /**
   * Generates a random {@link EmployeesRecord} with no defined
   * {@link Employees#EMPL_PK}.<br />
   * You'll have to either set or {@link TableRecord#reset(Field)} it before
   * persisting to the database.
   *
   * @param uniqueId
   *          A unique integer value to be used for populating this
   *          {@link EmployeesRecord}'s {@link Employees#EMPL_EXTERNAL_ID}
   *          field.
   * @return an {@link EmployeesRecord} with <strong>NO</strong>
   *         {@link Employees#EMPL_PK} defined.
   */
  public EmployeesRecord employee(final Integer uniqueId) {
    final boolean isMale = RANDOM.nextBoolean();
    final String firstName = FakeRecords.anyFrom(FakeRecords.FIRST_NAMES.get(isMale ? "male" : "female"));
    final String surname = FakeRecords.anyFrom(FakeRecords.LAST_NAMES);
    return new EmployeesRecord(null, firstName, surname, FakeRecords.anyWithin(this.dobRange), Boolean.valueOf(RANDOM.nextBoolean()),
                               FakeRecords.asEmail(String.format("%s.%s", firstName, surname)), "", Boolean.valueOf(isMale),
                               String.format("E%04d", uniqueId));
  }

  public Table<Record4<Integer, Date, String, String>> sessions(final String outcome, final int amount) {
    return DSL.values(IntStream.range(0, amount)
        .mapToObj(i -> {
          final Date date;
          switch (outcome) {
            case Constants.TRNG_OUTCOME_CANCELLED:
              date = FakeRecords.anyWithin(this.cancelledTraining);
              break;
            case Constants.TRNG_OUTCOME_COMPLETED:
              date = FakeRecords.anyWithin(this.completedTraining);
              break;
            case Constants.TRNG_OUTCOME_SCHEDULED:
              date = FakeRecords.anyWithin(this.scheduledTraining);
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
    return String.format("%s@demo.orca-solution.com", Normalizer.normalize(userName, Normalizer.Form.NFKD)
        .replaceAll("[^\\p{ASCII}a-zA-Z.-]", "")
        .replaceAll("[ ']+", "-").toLowerCase());
  }

  private static String anyFrom(final List<String> source) {
    return source.get(RANDOM.nextInt(source.size()));
  }

  private static Date anyWithin(final Range<LocalDate> dateRange) {
    return Date.valueOf(LocalDate
        .ofEpochDay(RANDOM.nextLong(dateRange.lowerEndpoint().toEpochDay(), dateRange.upperEndpoint().toEpochDay())));
  }
}
