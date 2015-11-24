package org.ccjmne.faomaintenance.api.rest;

import static org.ccjmne.faomaintenance.jooq.classes.Tables.DEPARTMENTS;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.EMPLOYEES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.EMPLOYEES_ROLES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.SITES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.SITES_EMPLOYEES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.UPDATES;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.ccjmne.faomaintenance.api.utils.SQLDateFormat;
import org.ccjmne.faomaintenance.jooq.classes.Sequences;
import org.ccjmne.faomaintenance.jooq.classes.tables.records.SitesEmployeesRecord;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.InsertValuesStep3;
import org.jooq.impl.DSL;
import org.jooq.tools.csv.CSVReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("update")
public class UpdateEndpoint {

	private static final Logger LOGGER = LoggerFactory.getLogger(UpdateEndpoint.class);
	private static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("#.##");

	private final DSLContext ctx;
	private final SQLDateFormat dateFormat;
	private final StatisticsEndpoint statistics;
	private final ResourcesEndpoint resources;

	@Inject
	public UpdateEndpoint(final DSLContext ctx, final SQLDateFormat dateFormat, final StatisticsEndpoint statistics, final ResourcesEndpoint resources) {
		this.dateFormat = dateFormat;
		this.ctx = ctx;
		this.statistics = statistics;
		this.resources = resources;
	}

	@PUT
	@Path("departments/{dept_pk}")
	@Consumes(MediaType.APPLICATION_JSON)
	public boolean updateDepartment(@PathParam("dept_pk") final Integer dept_pk, final Map<String, String> dept) {
		if (this.ctx.fetchExists(DEPARTMENTS, DEPARTMENTS.DEPT_PK.eq(dept_pk))) {
			this.ctx.update(DEPARTMENTS)
					.set(DEPARTMENTS.DEPT_NAME, dept.get(DEPARTMENTS.DEPT_NAME.getName()))
					.set(DEPARTMENTS.DEPT_ID, dept.get(DEPARTMENTS.DEPT_ID.getName()))
					.where(DEPARTMENTS.DEPT_PK.eq(dept_pk)).execute();
			return false;
		}

		this.ctx.insertInto(DEPARTMENTS, DEPARTMENTS.DEPT_PK, DEPARTMENTS.DEPT_NAME, DEPARTMENTS.DEPT_ID)
				.values(dept_pk, dept.get(DEPARTMENTS.DEPT_NAME.getName()), dept.get(DEPARTMENTS.DEPT_ID.getName())).execute();
		return true;
	}

	@PUT
	@Path("sites/{site_pk}")
	@Consumes(MediaType.APPLICATION_JSON)
	public boolean updateSite(@PathParam("site_pk") final String site_pk, final Map<String, String> site) {
		if (this.ctx.fetchExists(SITES, SITES.SITE_PK.eq(site_pk))) {
			this.ctx.update(SITES)
					.set(SITES.SITE_NAME, site.get(SITES.SITE_NAME.getName()))
					.set(SITES.SITE_DEPT_FK, Integer.valueOf(site.get(SITES.SITE_DEPT_FK.getName())))
					.set(SITES.SITE_PK, site.getOrDefault(SITES.SITE_PK.getName(), site_pk))
					.where(SITES.SITE_PK.eq(site_pk)).execute();
			return false;
		}

		this.ctx.insertInto(SITES, SITES.SITE_PK, SITES.SITE_NAME, SITES.SITE_DEPT_FK)
				.values(site_pk, site.get(SITES.SITE_NAME.getName()), Integer.valueOf(site.get(SITES.SITE_DEPT_FK.getName()))).execute();
		return true;
	}

	@DELETE
	@Path("departments/{dept_pk}")
	public boolean deleteDept(@PathParam("dept_pk") final Integer dept_pk) {
		final boolean exists = this.ctx.selectFrom(DEPARTMENTS).where(DEPARTMENTS.DEPT_PK.equal(dept_pk)).fetch().isNotEmpty();
		if (exists) {
			try {
				this.resources.listSites(dept_pk, null, false).forEach(site -> deleteSite(site.getValue(SITES.SITE_PK)));
			} catch (IllegalArgumentException | ParseException e) {
				// Should *never* happen
				e.printStackTrace();
			}

			this.ctx.delete(DEPARTMENTS).where(DEPARTMENTS.DEPT_PK.eq(dept_pk)).execute();
		}

		return exists;
	}

	@DELETE
	@Path("sites/{site_pk}")
	public boolean deleteSite(@PathParam("site_pk") final String site_pk) {
		final boolean exists = this.ctx.selectFrom(SITES).where(SITES.SITE_PK.equal(site_pk)).fetch().isNotEmpty();
		if (exists) {
			this.ctx.delete(SITES).where(SITES.SITE_PK.eq(site_pk)).execute();
			this.ctx.delete(SITES_EMPLOYEES).where(SITES_EMPLOYEES.SIEM_SITE_FK.eq(site_pk)).execute();
			this.statistics.invalidateSitesStats(Collections.singleton(site_pk));
		}

		return exists;
	}

	@POST
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Produces(MediaType.APPLICATION_JSON)
	@Path("parse")
	public Response parse(
							@QueryParam("pageNumber") final int pageNumber,
							@QueryParam("pageName") final String pageName,
							@FormDataParam("file") final InputStream file,
							@FormDataParam("file") final FormDataContentDisposition fileDisposition) {
		try {
			switch (fileDisposition.getFileName().substring(fileDisposition.getFileName().lastIndexOf(".") + 1)) {
				case "csv":
					try (final CSVReader reader = new CSVReader(new InputStreamReader(file))) {
						final List<String[]> list = reader.readAll();
						return Response.status(Status.OK).entity(list.toArray(new String[list.size()][])).build();
					}
				case "xls":
					try (final Workbook workbook = new HSSFWorkbook(file)) {
						return Response.status(Status.OK).entity(readSheet(workbook, pageNumber, pageName)).build();
					}
				case "xlsx":
					try (final Workbook workbook = new XSSFWorkbook(file)) {
						return Response.status(Status.OK).entity(readSheet(workbook, pageNumber, pageName)).build();
					}
				default:
					return Response.status(Status.BAD_REQUEST).entity("Uploaded file was neither a .xls nor a .xlsx file.").build();
			}
		} catch (final IOException e) {
			UpdateEndpoint.LOGGER.error(String.format("Could not parse file '%s'.", fileDisposition.getFileName()), e);
			return Response.status(Status.BAD_REQUEST).entity(String.format("Could not parse file '%s'.", fileDisposition.getFileName())).build();
		}
	}

	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	public Response process(final List<Map<String, String>> employees) {
		try {
			this.ctx.transaction(config -> {
				final DSLContext transactionCtx = DSL.using(config);
				final Integer updt_pk = new Integer(transactionCtx.nextval(Sequences.UPDATES_UPDT_PK_SEQ).intValue());
				transactionCtx.insertInto(UPDATES).set(UPDATES.UPDT_PK, updt_pk).set(UPDATES.UPDT_DATE, new java.sql.Date(new Date().getTime())).execute();

				final InsertValuesStep3<SitesEmployeesRecord, Integer, String, String> query = transactionCtx
						.insertInto(SITES_EMPLOYEES, SITES_EMPLOYEES.SIEM_UPDT_FK, SITES_EMPLOYEES.SIEM_SITE_FK, SITES_EMPLOYEES.SIEM_EMPL_FK);
				for (final Map<String, String> employee : employees) {
					query.values(updt_pk, employee.get(SITES_EMPLOYEES.SIEM_SITE_FK.getName()), updateEmployee(employee, transactionCtx));
				}

				query.execute();

				// Remove all privileges of the remaining employees
				transactionCtx
						.delete(EMPLOYEES_ROLES)
						.where(
								EMPLOYEES_ROLES.EMPL_PK.notIn(transactionCtx.select(SITES_EMPLOYEES.SIEM_EMPL_FK).from(SITES_EMPLOYEES)
										.where(SITES_EMPLOYEES.SIEM_UPDT_FK.eq(updt_pk)))).execute();

				// ... and set their site to #0 ('unassigned')
				transactionCtx.insertInto(SITES_EMPLOYEES, SITES_EMPLOYEES.SIEM_EMPL_FK, SITES_EMPLOYEES.SIEM_SITE_FK, SITES_EMPLOYEES.SIEM_UPDT_FK)
						.select(
								transactionCtx.select(
														SITES_EMPLOYEES.SIEM_EMPL_FK,
														DSL.val("0"),
														DSL.val(updt_pk)).from(SITES_EMPLOYEES)
										.where(SITES_EMPLOYEES.SIEM_EMPL_FK
												.notIn(transactionCtx.select(SITES_EMPLOYEES.SIEM_EMPL_FK).from(SITES_EMPLOYEES)
														.where(SITES_EMPLOYEES.SIEM_UPDT_FK.eq(updt_pk))))).execute();
			});
		} catch (final Exception e) {
			return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build();
		}

		this.statistics.invalidateSitesStats();
		return Response.ok().build();
	}

	@SuppressWarnings("unchecked")
	private String updateEmployee(final Map<String, String> employee, final DSLContext context) throws ParseException {
		final String empl_pk = employee.get(EMPLOYEES.EMPL_PK.getName());
		final Map<Object, Object> record = new HashMap<>();
		record.put(EMPLOYEES.EMPL_FIRSTNAME, employee.get(EMPLOYEES.EMPL_FIRSTNAME.getName()));
		record.put(EMPLOYEES.EMPL_SURNAME, employee.get(EMPLOYEES.EMPL_SURNAME.getName()));
		record.put(EMPLOYEES.EMPL_DOB, this.dateFormat.parseSql(employee.get(EMPLOYEES.EMPL_DOB.getName())));
		record.put(EMPLOYEES.EMPL_PERMANENT, Boolean.valueOf("CDI".equalsIgnoreCase(employee.get(EMPLOYEES.EMPL_PERMANENT.getName()))));
		record.put(EMPLOYEES.EMPL_ADDR, employee.get(EMPLOYEES.EMPL_ADDR.getName()));

		if (context.fetchExists(EMPLOYEES, EMPLOYEES.EMPL_PK.eq(empl_pk))) {
			context.update(EMPLOYEES).set((Map<? extends Field<?>, ?>) record).where(EMPLOYEES.EMPL_PK.eq(empl_pk)).execute();
		} else {
			record.put(EMPLOYEES.EMPL_PK, empl_pk);
			context.insertInto(EMPLOYEES).set((Map<? extends Field<?>, ?>) record).execute();
		}

		return empl_pk;
	}

	private List<List<String>> readSheet(final Workbook workbook, final int pageNumber, final String pageName) {
		final FormulaEvaluator evaluator = workbook.getCreationHelper().createFormulaEvaluator();
		final Sheet sheet = ((pageName != null) && !pageName.isEmpty()) ? workbook.getSheet(pageName) : workbook.getSheetAt(pageNumber);

		final List<List<String>> res = new ArrayList<>();
		final int lastColNum = sheet.getRow(sheet.getFirstRowNum()).getLastCellNum();
		for (final Row row : sheet) {
			final List<String> line = new ArrayList<>(lastColNum);
			for (int col = 0; col < lastColNum; col++) {
				line.add(getStringValue(row.getCell(col), evaluator));
			}

			if (!line.stream().allMatch(entry -> entry.isEmpty())) {
				res.add(line);
			}
		}

		return res;
	}

	private String getStringValue(final Cell cell, final FormulaEvaluator evaluator) {
		if (cell == null) {
			return "";
		}

		switch (cell.getCellType()) {
			case Cell.CELL_TYPE_NUMERIC:
				if (DateUtil.isCellDateFormatted(cell)) {
					return this.dateFormat.format(cell.getDateCellValue());
				}

				return DECIMAL_FORMAT.format(cell.getNumericCellValue());

			case Cell.CELL_TYPE_ERROR:
			case Cell.CELL_TYPE_BLANK:
				return "";

			case Cell.CELL_TYPE_FORMULA:
				return evaluator.evaluate(cell).getStringValue();

			default:
				return cell.getStringCellValue();
		}
	}
}