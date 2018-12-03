package org.ccjmne.orca.api.rest.edit;

import static org.ccjmne.orca.jooq.classes.Tables.EMPLOYEES;
import static org.ccjmne.orca.jooq.classes.Tables.EMPLOYEES_VOIDINGS;

import java.sql.Date;
import java.util.Map;
import java.util.Optional;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.ccjmne.orca.api.inject.business.QueryParameters;
import org.ccjmne.orca.api.inject.business.Restrictions;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Param;
import org.jooq.Record;
import org.jooq.impl.DSL;

@Path("employees-notes")
public class EmployeesNotesEndpoint {

  private final DSLContext ctx;

  private final Field<Date>              date;
  private final Param<Integer>           employee;
  private final Optional<Param<Integer>> certificate;

  @Inject
  public EmployeesNotesEndpoint(final DSLContext ctx, final Restrictions restrictions, final QueryParameters parameters) {
    if (!restrictions.canManageEmployeeNotes()) {
      throw new ForbiddenException();
    }

    if (!parameters.has(QueryParameters.EMPLOYEE)) {
      throw new IllegalArgumentException("Missing 'employee' identifier.");
    }

    this.employee = parameters.get(QueryParameters.EMPLOYEE);
    this.date = parameters.get(QueryParameters.DATE);
    this.certificate = parameters.of(QueryParameters.CERTIFICATE);
    this.ctx = ctx;
  }

  @PUT
  @Path("{employee}")
  @Consumes(MediaType.APPLICATION_JSON)
  public void setNotes(final Map<String, String> data) {
    this.ctx.update(EMPLOYEES).set(EMPLOYEES.EMPL_NOTES, data.get(EMPLOYEES.EMPL_NOTES.getName())).where(EMPLOYEES.EMPL_PK.eq(this.employee)).execute();
  }

  @GET
  @Path("{employee}/voidings")
  @Produces(MediaType.APPLICATION_JSON)
  public Map<Integer, ? extends Record> listEmployeesVoidings() {
    return this.ctx
        .select(EMPLOYEES_VOIDINGS.EMVO_DATE, EMPLOYEES_VOIDINGS.EMVO_REASON).from(EMPLOYEES_VOIDINGS)
        .where(EMPLOYEES_VOIDINGS.EMVO_EMPL_FK.eq(this.employee))
        .fetchMap(EMPLOYEES_VOIDINGS.EMVO_CERT_FK);
  }

  @POST
  @Path("{employee}/voidings")
  @Consumes(MediaType.APPLICATION_JSON)
  public void optOut(final Map<String, String> data) {
    if (!this.certificate.isPresent()) {
      throw new IllegalArgumentException("Missing 'certificate' parameter.");
    }

    final String reason = data.get(EMPLOYEES_VOIDINGS.EMVO_REASON.getName());
    this.ctx
        .insertInto(EMPLOYEES_VOIDINGS,
                    EMPLOYEES_VOIDINGS.EMVO_EMPL_FK, EMPLOYEES_VOIDINGS.EMVO_CERT_FK, EMPLOYEES_VOIDINGS.EMVO_DATE, EMPLOYEES_VOIDINGS.EMVO_REASON)
        .values(this.employee, this.certificate.get(), this.date, DSL.val(reason))
        .onDuplicateKeyUpdate().set(EMPLOYEES_VOIDINGS.EMVO_DATE, this.date).set(EMPLOYEES_VOIDINGS.EMVO_REASON, reason)
        .execute();
  }

  @DELETE
  @Path("{employee}/voidings")
  public void optBackIn() {
    if (!this.certificate.isPresent()) {
      throw new IllegalArgumentException("Missing 'certificate' parameter.");
    }

    this.ctx.deleteFrom(EMPLOYEES_VOIDINGS).where(EMPLOYEES_VOIDINGS.EMVO_EMPL_FK.eq(this.employee))
        .and(EMPLOYEES_VOIDINGS.EMVO_CERT_FK.eq(this.certificate.get())).execute();
  }
}
