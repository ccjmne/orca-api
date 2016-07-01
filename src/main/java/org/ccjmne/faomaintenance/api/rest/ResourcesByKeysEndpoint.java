package org.ccjmne.faomaintenance.api.rest;

import static org.ccjmne.faomaintenance.jooq.classes.Tables.CERTIFICATES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.DEPARTMENTS;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.EMPLOYEES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.SITES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGS;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGTYPES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGTYPES_CERTIFICATES;

import java.text.ParseException;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

import org.ccjmne.faomaintenance.jooq.classes.tables.records.CertificatesRecord;
import org.ccjmne.faomaintenance.jooq.classes.tables.records.TrainingtypesRecord;
import org.jooq.Record;

@Path("resources-by-keys")
public class ResourcesByKeysEndpoint {

	final ResourcesEndpoint resources;

	@Inject
	public ResourcesByKeysEndpoint(final ResourcesEndpoint resources) {
		this.resources = resources;
	}

	@GET
	@Path("employees")
	public Map<String, Record> listEmployees(
												@QueryParam("site") final String site_pk,
												@QueryParam("date") final String dateStr,
												@QueryParam("training") final String trng_pk) throws ParseException {
		return this.resources.listEmployees(site_pk, dateStr, trng_pk).intoMap(EMPLOYEES.EMPL_PK);
	}

	@GET
	@Path("sites")
	public Map<String, Record> listSites(
											@QueryParam("department") final Integer department,
											@QueryParam("employee") final String employee,
											@QueryParam("date") final String dateStr,
											@QueryParam("unlisted") final boolean unlisted) throws ParseException {
		return this.resources.listSites(department, employee, dateStr, unlisted).intoMap(SITES.SITE_PK);
	}

	@GET
	@Path("trainings")
	public Map<Integer, Record> listTrainingsByKeys(
													@QueryParam("employee") final String empl_pk,
													@QueryParam("type") final List<Integer> types,
													@QueryParam("date") final String dateStr,
													@QueryParam("from") final String fromStr,
													@QueryParam("to") final String toStr,
													@QueryParam("completed") final Boolean completedOnly) throws ParseException {
		return this.resources.listTrainings(empl_pk, types, dateStr, fromStr, toStr, completedOnly).intoMap(TRAININGS.TRNG_PK);
	}

	@GET
	@Path("departments")
	public Map<Integer, ? extends Record> listDepartments(@QueryParam("unlisted") final boolean unlisted) {
		return this.resources.listDepartments(unlisted).intoMap(DEPARTMENTS.DEPT_PK);
	}

	@GET
	@Path("trainingtypes")
	public Map<Integer, TrainingtypesRecord> listTrainingTypes() {
		return this.resources.listTrainingTypes().intoMap(TRAININGTYPES.TRTY_PK);
	}

	@GET
	@Path("trainingtypes_certificates")
	public Map<Integer, List<Integer>> listTrainingtypesCertificates() {
		return this.resources.listTrainingTypesCertificates().intoGroups(TRAININGTYPES_CERTIFICATES.TTCE_TRTY_FK, TRAININGTYPES_CERTIFICATES.TTCE_CERT_FK);
	}

	@GET
	@Path("certificates")
	public Map<Integer, CertificatesRecord> listCertificates() {
		return this.resources.listCertificates().intoMap(CERTIFICATES.CERT_PK);
	}
}
