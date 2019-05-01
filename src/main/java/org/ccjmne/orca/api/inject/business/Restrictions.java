package org.ccjmne.orca.api.inject.business;

import static org.ccjmne.orca.jooq.codegen.Tables.SITES_EMPLOYEES;
import static org.ccjmne.orca.jooq.codegen.Tables.TRAINERPROFILES_TRAININGTYPES;
import static org.ccjmne.orca.jooq.codegen.Tables.USERS;
import static org.ccjmne.orca.jooq.codegen.Tables.USERS_ROLES;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Context;

import org.ccjmne.orca.api.utils.Constants;
import org.ccjmne.orca.api.utils.Fields;
import org.ccjmne.orca.jooq.codegen.tables.records.UsersRecord;
import org.ccjmne.orca.jooq.codegen.tables.records.UsersRolesRecord;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;

import com.fasterxml.jackson.annotation.JsonGetter;

/**
 * Provides tools to manage restrictions of resources access or manipulation
 * provided an authenticated {@link HttpServletRequest} or a specific user ID.
 */
// TODO: FIX ACCESS LEVELS now that DEPARTMENTS don't exist anymore
public class Restrictions {

  private final DSLContext ctx;

  private final String        userId;
  private final boolean       accessTrainings;
  private final boolean       accessAllSites;
  private final boolean       manageEmployeeNotes;
  private final boolean       manageSitesAndTags;
  private final boolean       manageCertificates;
  private final boolean       manageUsers;
  private final boolean       manageOwnAccount;
  private final List<Integer> accessibleSites;
  private final List<Integer> manageableTypes;

  @Inject
  public Restrictions(@Context final HttpServletRequest request, final DSLContext ctx) {
    this(request.getRemoteUser(), ctx);
  }

  public static Restrictions forUser(final String user_id, final DSLContext ctx) {
    return new Restrictions(user_id, ctx);
  }

  private Restrictions(final String user_id, final DSLContext ctx) {
    this.ctx = ctx;
    this.userId = user_id;
    final Map<String, UsersRolesRecord> roles = ctx.selectFrom(USERS_ROLES).where(USERS_ROLES.USER_ID.eq(user_id)).fetchMap(USERS_ROLES.USRO_TYPE);
    this.manageOwnAccount = roles.containsKey(Constants.ROLE_USER);
    if (roles.containsKey(Constants.ROLE_ADMIN)) {
      this.manageEmployeeNotes = roles.get(Constants.ROLE_ADMIN).getUsroLevel().compareTo(Integer.valueOf(1)) >= 0;
      this.manageSitesAndTags = roles.get(Constants.ROLE_ADMIN).getUsroLevel().compareTo(Integer.valueOf(2)) >= 0;
      this.manageCertificates = roles.get(Constants.ROLE_ADMIN).getUsroLevel().compareTo(Integer.valueOf(3)) >= 0;
      this.manageUsers = roles.get(Constants.ROLE_ADMIN).getUsroLevel().equals(Integer.valueOf(4));
    } else {
      this.manageEmployeeNotes = false;
      this.manageSitesAndTags = false;
      this.manageCertificates = false;
      this.manageUsers = false;
    }

    this.accessTrainings = roles.containsKey(Constants.ROLE_ACCESS)
        && Constants.ACCESS_LEVEL_TRAININGS.equals(roles.get(Constants.ROLE_ACCESS).getUsroLevel());
    this.accessAllSites = roles.containsKey(Constants.ROLE_ACCESS)
        && (Constants.ACCESS_LEVEL_ALL_SITES.compareTo(roles.get(Constants.ROLE_ACCESS).getUsroLevel()) <= 0);

    final UsersRecord user = ctx.selectFrom(USERS).where(USERS.USER_ID.eq(user_id)).fetchOne();
    this.accessibleSites = this.listAccessibleSites(user, roles.get(Constants.ROLE_ACCESS));
    this.manageableTypes = this.listManageableTypes(roles.get(Constants.ROLE_TRAINER));
  }

  private List<Integer> listAccessibleSites(final UsersRecord user, final UsersRolesRecord role) {
    if ((role == null)
        || (Constants.ACCESS_LEVEL_ALL_SITES.compareTo(role.getUsroLevel()) <= 0)
        || Constants.ACCESS_LEVEL_ONESELF.equals(role.getUsroLevel())) {
      return Collections.EMPTY_LIST;
    }

    final Integer site;
    if (user.getUserType().equals(Constants.USERTYPE_EMPLOYEE)) {
      site = this.ctx.selectFrom(SITES_EMPLOYEES)
          .where(SITES_EMPLOYEES.SIEM_EMPL_FK.eq(user.getUserEmplFk())
              .and(SITES_EMPLOYEES.SIEM_UPDT_FK.eq(Fields.selectUpdate(DSL.currentDate())))
              .and(SITES_EMPLOYEES.SIEM_SITE_FK.ne(Constants.DECOMMISSIONED_SITE)))
          .fetchOne(SITES_EMPLOYEES.SIEM_SITE_FK);
    } else {
      // Constants.USERTYPE_SITE
      site = user.getUserSiteFk();
    }

    return Collections.singletonList(site);
  }

  private List<Integer> listManageableTypes(final UsersRolesRecord role) {
    if (role == null) {
      return Collections.EMPTY_LIST;
    }

    return this.ctx.selectFrom(TRAINERPROFILES_TRAININGTYPES)
        .where(TRAINERPROFILES_TRAININGTYPES.TPTT_TRPR_FK.eq(role.getUsroTrprFk()))
        .fetch(TRAINERPROFILES_TRAININGTYPES.TPTT_TRTY_FK);
  }

  public String getUserId() {
    return this.userId;
  }

  /**
   * There is no access restriction to sites by tag.<br />
   * For now, users can access either:
   * <ul>
   * <li>no site</li>
   * <li>a single site (theirs)</li>
   * <li>all sites</li>
   * </ul>
   *
   * @param tags_pk
   *          The tag whose sites the user is attempting to query for
   */
  public boolean canAccessSitesWith(final Integer tags_pk) {
    return this.canAccessAllSites();
  }

  public boolean canAccessSite(final Integer site_pk) {
    return this.accessAllSites || (this.accessibleSites.contains(site_pk));
  }

  @JsonGetter
  public boolean canAccessTrainings() {
    return this.accessTrainings;
  }

  /**
   * Checks whether the current {@link HttpServletRequest} can access all
   * existing sites.<br />
   * Should this method return <code>true</code>,
   * {@link Restrictions#getAccessibleSites()} would return an empty
   * <code>List</code>.
   *
   * @return <code>true</code> if all sites are accessible to the injected
   *         request.
   */
  @JsonGetter
  public boolean canAccessAllSites() {
    return this.accessAllSites;
  }

  @JsonGetter
  public boolean canManageEmployeeNotes() {
    return this.manageEmployeeNotes;
  }

  @JsonGetter
  public boolean canManageSitesAndTags() {
    return this.manageSitesAndTags;
  }

  @JsonGetter
  public boolean canManageCertificates() {
    return this.manageCertificates;
  }

  @JsonGetter
  public boolean canManageUsers() {
    return this.manageUsers;
  }

  /**
   * Same implementation as {@link Restrictions#canManageUsers}: the same
   * authorisation is required.
   */
  @JsonGetter
  public boolean canManageClient() {
    return this.manageUsers;
  }

  /**
   * If <code>true</code>, then the user can change its own ID and password.
   */
  public boolean canManageOwnAccount() {
    return this.manageOwnAccount;
  }

  /**
   * Get the sites the current {@link HttpServletRequest}'s scope should be
   * restricted to or an <b>empty list</b> if all sites are accessible.
   *
   * @return The list of accessible sites if relevant.
   * @see {@link Restrictions#canAccessAllSites()}
   */
  public List<Integer> getAccessibleSites() {
    return this.accessibleSites;
  }

  public List<Integer> getManageableTypes() {
    return this.manageableTypes;
  }
}
