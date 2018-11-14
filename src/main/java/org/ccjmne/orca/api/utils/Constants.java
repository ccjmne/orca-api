package org.ccjmne.orca.api.utils;

import static org.ccjmne.orca.jooq.classes.Tables.UPDATES;
import static org.ccjmne.orca.jooq.classes.Tables.USERS;

import java.sql.Date;
import java.time.LocalDate;
import java.time.Month;
import java.time.temporal.TemporalAdjusters;
import java.util.Arrays;
import java.util.List;

import org.ccjmne.orca.jooq.classes.tables.records.UpdatesRecord;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.Select;
import org.jooq.SelectQuery;
import org.jooq.TableLike;
import org.jooq.impl.DSL;

public class Constants {

	/**
	 * If you are trying to use the maximum value as some kind of flag such as
	 * "undetermined future date" to avoid a NULL, instead choose some arbitrary
	 * date far enough in the future to exceed any legitimate value but not so
	 * far as to exceed the limits of any database you are possibly going to
	 * use. Define a constant for this value in your Java code and in your
	 * database, and document thoroughly.
	 *
	 * @see <a href=
	 *      "http://stackoverflow.com/questions/41301892/insert-the-max-date-independent-from-database">
	 *      Insert the max date (independent from database)</a>
	 */
	public static final Date DATE_INFINITY = Date.valueOf(LocalDate.of(9999, Month.JANUARY, 1).with(TemporalAdjusters.lastDayOfYear()));

	// ---- API CONSTANTS
	public static final String FIELDS_ALL = "all";
	public static final String DATE_INFINITY_LITERAL = "infinity";
	private static final Integer NO_UPDATE = Integer.valueOf(-1);

	public static final String STATUS_SUCCESS = "success";
	public static final String STATUS_WARNING = "warning";
	public static final String STATUS_DANGER = "danger";

	public static final String TAGS_VALUE_UNIVERSAL = "*";
	// Actually, it's not true anymore, it's just an actual JSON "null" leaf
	// TODO: Maybe delete this property
	public static final String TAGS_VALUE_NONE = String.valueOf((Object) null);

	public static final String SORT_DIRECTION_DESC = "desc"; // case-insensitive
	public static final String FILTER_VALUE_NULL = "null";
	// ----

	// ---- DATABASE CONSTANTS
	public static final String TRNG_OUTCOME_CANCELLED = "CANCELLED";
	public static final String TRNG_OUTCOME_COMPLETED = "COMPLETED";
	public static final String TRNG_OUTCOME_SCHEDULED = "SCHEDULED";
	public static final List<String> TRAINING_OUTCOMES = Arrays
			.asList(Constants.TRNG_OUTCOME_CANCELLED, Constants.TRNG_OUTCOME_COMPLETED, Constants.TRNG_OUTCOME_SCHEDULED);

	public static final String EMPL_OUTCOME_CANCELLED = "CANCELLED";
	public static final String EMPL_OUTCOME_FLUNKED = "FLUNKED";
	public static final String EMPL_OUTCOME_MISSING = "MISSING";
	public static final String EMPL_OUTCOME_PENDING = "PENDING";
	public static final String EMPL_OUTCOME_VALIDATED = "VALIDATED";

	public static final String TAGS_TYPE_STRING = "s";
	public static final String TAGS_TYPE_BOOLEAN = "b";

	public static final String USER_ROOT = "root";

	public static final Integer EMPLOYEE_ROOT = Integer.valueOf(0);
	public static final Integer DECOMMISSIONED_SITE = Integer.valueOf(0);
	public static final Integer DEFAULT_TRAINERPROFILE = Integer.valueOf(0);

	public static final String ROLE_USER = "user";
	public static final String ROLE_ACCESS = "access";
	public static final String ROLE_TRAINER = "trainer";
	public static final String ROLE_ADMIN = "admin";

	public static final String USERTYPE_EMPLOYEE = "employee";
	public static final String USERTYPE_SITE = "site";

	public static final Integer ACCESS_LEVEL_TRAININGS = Integer.valueOf(4);
	public static final Integer ACCESS_LEVEL_ALL_SITES = Integer.valueOf(3);
	public static final Integer ACCESS_LEVEL_SITE = Integer.valueOf(2);
	public static final Integer ACCESS_LEVEL_ONESELF = Integer.valueOf(1);
	// ----

	// ---- SUBQUERIES AND FIELDS
	// TODO: move to new static class?
	public static Field<?>[] USERS_FIELDS = new Field<?>[] { USERS.USER_ID, USERS.USER_TYPE, USERS.USER_EMPL_FK, USERS.USER_SITE_FK };
	public static final Field<Integer> CURRENT_UPDATE = Constants.selectUpdate(null);

	/**
	 * Returns a select sub-query that maps the results of the provided
	 * {@code query} to the sole specified {@code field}.
	 *
	 * @param <T>
	 *            The specified field type.
	 * @param field
	 *            The field to extract.
	 * @param query
	 *            The original query to use as a data source.
	 * @return A sub-query containing the sole specified {@code field} from the
	 *         given {@code query}.
	 */
	public static <T> Select<Record1<T>> select(final Field<T> field, final SelectQuery<?> query) {
		final TableLike<? extends Record> table = query.asTable();
		return DSL.select(table.field(field)).from(table);
	}

	/**
	 * Clone the supplied {@code fields} stripped from their {@code Table}
	 * qualification, for referencing within sub-queries.<br />
	 * Alternatively, use {@link TableLike#fields(Field...)}.
	 *
	 * @param fields
	 *            The array of {@code Field}s to be cloned
	 * @return The un-qualified {@code fields}
	 */
	@SafeVarargs
	public static <T> Field<? extends T>[] unqualify(final Field<? extends T>... fields) {
		return Arrays.stream(fields).map(Field::getUnqualifiedName).map(DSL::field).toArray(Field[]::new);
	}

	/**
	 * Clone the supplied {@code field} stripped from its {@code Table}
	 * qualification, for referencing within sub-queries.<br />
	 * Alternatively, use {@link TableLike#field(Field)}.
	 *
	 * @param field
	 *            The {@code Field}s to be cloned
	 * @return The un-qualified {@code field}
	 */
	public static <T> Field<T> unqualify(final Field<T> field) {
		return DSL.field(field.getUnqualifiedName(), field.getType());
	}

	public static Field<Date> fieldDate(final String dateStr) {
		return dateStr != null ? DSL.date(dateStr) : DSL.currentDate();
	}

	/**
	 * Returns a sub-query selecting the <strong>primary key</strong> of the
	 * {@link UpdatesRecord} that is or was relevant at a given date, or today
	 * if no date is specified.
	 *
	 * @param dateStr
	 *            The date for which to compute the relevant
	 *            {@link UpdatesRecord}, in the <code>"YYYY-MM-DD"</code>
	 *            format.
	 * @return The relevant {@link UpdatesRecord}'s primary key or
	 *         {@value Constants#NO_UPDATE} if no such update found.
	 */
	public static Field<Integer> selectUpdate(final String dateStr) {
		return DSL.coalesce(
							DSL.select(UPDATES.UPDT_PK).from(UPDATES).where(UPDATES.UPDT_DATE.eq(DSL.select(DSL.max(UPDATES.UPDT_DATE)).from(UPDATES)
									.where(UPDATES.UPDT_DATE.le(Constants.fieldDate(dateStr)))))
									.asField(),
							NO_UPDATE);
	}
}
