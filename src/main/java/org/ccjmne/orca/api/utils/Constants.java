package org.ccjmne.orca.api.utils;

import java.time.LocalDate;
import java.time.Month;
import java.time.temporal.TemporalAdjusters;
import java.util.Arrays;
import java.util.List;

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
  public static final LocalDate DATE_NEVER  = LocalDate.of(9999, Month.JANUARY, 1).with(TemporalAdjusters.lastDayOfYear());
  public static final String    DATE_FORMAT = "yyyy-MM-dd";

  // ---- API CONSTANTS
  public static final String FIELDS_ALL         = "all";
  public static final String DATE_NEVER_LITERAL = "infinity";

  public static final String SITE_STATUS_OK      = "ok";
  public static final String SITE_STATUS_OKAYISH = "okayish";
  public static final String SITE_STATUS_KO      = "ko";

  public static final String EMPL_STATUS_VALID    = "valid";   // lasting + expiring
  public static final String EMPL_STATUS_LASTING  = "lasting";
  public static final String EMPL_STATUS_EXPIRING = "expiring";

  public static final String EMPL_STATUS_INVALID = "invalid"; // expired + voided
  public static final String EMPL_STATUS_EXPIRED = "expired";
  public static final String EMPL_STATUS_VOIDED  = "voided";

  public static final String TAGS_VALUE_UNIVERSAL = "*";
  public static final String TAGS_VALUE_NONE      = String.valueOf((Object) null);

  public static final String SORT_DIRECTION_DESC = "desc"; // case-insensitive
  public static final String FILTER_VALUE_NULL   = "null";
  // ----

  // ---- DATABASE CONSTANTS
  public static final String       TRNG_OUTCOME_CANCELLED = "CANCELLED";
  public static final String       TRNG_OUTCOME_COMPLETED = "COMPLETED";
  public static final String       TRNG_OUTCOME_SCHEDULED = "SCHEDULED";
  public static final List<String> TRAINING_OUTCOMES      = Arrays.asList(TRNG_OUTCOME_CANCELLED, TRNG_OUTCOME_COMPLETED, TRNG_OUTCOME_SCHEDULED);

  public static final String       EMPL_OUTCOME_CANCELLED = "CANCELLED";
  public static final String       EMPL_OUTCOME_FLUNKED   = "FLUNKED";
  public static final String       EMPL_OUTCOME_MISSING   = "MISSING";
  public static final String       EMPL_OUTCOME_PENDING   = "PENDING";
  public static final String       EMPL_OUTCOME_VALIDATED = "VALIDATED";
  public static final List<String> EMPLOYEES_OUTCOMES     = Arrays.asList(EMPL_OUTCOME_CANCELLED, EMPL_OUTCOME_FLUNKED, EMPL_OUTCOME_MISSING,
                                                                          EMPL_OUTCOME_PENDING, EMPL_OUTCOME_VALIDATED);

  public static final String TAGS_TYPE_STRING  = "s";
  public static final String TAGS_TYPE_BOOLEAN = "b";

  public static final String USER_ROOT = "root";

  public static final Integer EMPLOYEE_ROOT          = Integer.valueOf(0);
  public static final Integer DECOMMISSIONED_SITE    = Integer.valueOf(0);
  public static final Integer DEFAULT_TRAINERPROFILE = Integer.valueOf(0);

  public static final String ROLE_USER       = "user";
  public static final String ROLE_ACCESS     = "access";
  public static final String ROLE_INSTRUCTOR = "trainer"; // TODO: should be "instructor" probably
  public static final String ROLE_ADMIN      = "admin";

  public static final String USERTYPE_EMPLOYEE = "employee";
  public static final String USERTYPE_SITE     = "site";

  public static final Integer ACCESS_LEVEL_SESSIONS  = Integer.valueOf(4);
  public static final Integer ACCESS_LEVEL_ALL_SITES = Integer.valueOf(3);
  public static final Integer ACCESS_LEVEL_SITE      = Integer.valueOf(2);
  public static final Integer ACCESS_LEVEL_ONESELF   = Integer.valueOf(1);
}
