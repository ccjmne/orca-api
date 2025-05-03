package org.ccjmne.orca.api.utils;

import static org.ccjmne.orca.jooq.codegen.Tables.TRAININGTYPES_DEFS;
import static org.ccjmne.orca.jooq.codegen.Tables.UPDATES;
import static org.ccjmne.orca.jooq.codegen.Tables.USERS;

import java.sql.Date;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

import org.ccjmne.orca.jooq.codegen.tables.records.UpdatesRecord;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.Record2;
import org.jooq.Select;
import org.jooq.SelectQuery;
import org.jooq.Table;
import org.jooq.TableLike;
import org.jooq.impl.DSL;

public class Constants {

        /**
         * Postgres will yield 'infinity' for dates that are infinitely far in time,
         * and Java will interpret these as approximately +292278994-08-17 07:12:55,
         * which is the UTC date of 2^63-1 milliseconds since the epoch.
         *
         * It isn't however equal to LocalDate.MAX, which is +999999999-12-31,
         * nor to Instant.MAX, which would be 1000000000-12-31T23:59:59.999999999Z.
         *
         * For some reason (most likely time zones configuration), these
         * dates may be off by one. We remedy this issue by comparing the
         * Duration#between the LocalDateTimes: if they're within a day; that's
         * infinity (positive or negative).
         */
	public static final LocalDateTime DATE_INFINITY = new Date(Long.MAX_VALUE).toLocalDate().atStartOfDay();
	public static final LocalDateTime DATE_NEGATIVE_INFINITY = new Date(Long.MIN_VALUE).toLocalDate().atStartOfDay();

	// ---- API CONSTANTS
	public static final String FIELDS_ALL = "all";
	public static final String DATE_INFINITY_LITERAL = "infinity";
	public static final String DATE_NEGATIVE_INFINITY_LITERAL = "-infinity";
	private static final Integer NO_UPDATE = Integer.valueOf(-1);

	public static final String STATUS_SUCCESS = "success";
	public static final String STATUS_WARNING = "warning";
	public static final String STATUS_DANGER = "danger";

	public static final String TAGS_VALUE_UNIVERSAL = "*";
	public static final String TAGS_VALUE_NONE = "null";
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
	 * Unlike DSL#localDate, this handles `-infinity` and `infinity`.
	 */
	public static Field<LocalDate> fieldDate(final String dateStr) {
		if (dateStr == null) {
			return DSL.currentLocalDate();
		}
		return DSL.cast(DSL.val(dateStr), LocalDate.class);
	}

	public static Field<Integer> selectTypeDef(final Field<Integer> trty, final Field<LocalDate> date) {
		return DSL.select(TRAININGTYPES_DEFS.TTDF_PK)
			.from(TRAININGTYPES_DEFS)
			.where(TRAININGTYPES_DEFS.TTDF_TRTY_FK.eq(trty))
			.and(TRAININGTYPES_DEFS.TTDF_EFFECTIVE_FROM.le(date))
			.orderBy(TRAININGTYPES_DEFS.TTDF_EFFECTIVE_FROM.desc())
			.limit(1)
			.asField();
	}

    public static Field<Integer> effectiveTypeDefs(final Field<LocalDate> date) {
        final Table<Record2<Integer, Integer>> cte = DSL.select(
            TRAININGTYPES_DEFS.TTDF_PK,
            DSL.rowNumber().over(DSL.partitionBy(TRAININGTYPES_DEFS.TTDF_TRTY_FK).orderBy(TRAININGTYPES_DEFS.TTDF_EFFECTIVE_FROM.desc()))
        )
            .from(TRAININGTYPES_DEFS)
            .where(TRAININGTYPES_DEFS.TTDF_EFFECTIVE_FROM.le(date))
            .asTable("defs_chrono", "pk", "rank");

        return DSL.select(cte.field("pk", Integer.class))
            .from(cte)
            .where(cte.field("rank", Integer.class).eq(Integer.valueOf(1)))
            .asField();
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
