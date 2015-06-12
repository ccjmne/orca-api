package org.ccjmne.faomaintenance.api.rest;

import static org.ccjmne.faomaintenance.jooq.classes.Tables.EMPLOYEES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.SITES_EMPLOYEES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.UPDATES;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.ccjmne.faomaintenance.api.utils.SQLDateFormat;
import org.ccjmne.faomaintenance.jooq.classes.Sequences;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.tools.csv.CSVReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("import")
public class ImportEndpoint {

	private static final Logger LOGGER = LoggerFactory.getLogger(ImportEndpoint.class);

	private final DSLContext ctx;
	private final SQLDateFormat dateFormat;

	@Inject
	public ImportEndpoint(final DSLContext ctx, final SQLDateFormat dateFormat) {
		this.dateFormat = dateFormat;
		this.ctx = ctx;
	}

	@POST
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Produces(MediaType.APPLICATION_JSON)
	public Response postForm(
								@QueryParam("page") final int pageNumber,
								@FormDataParam("file") final InputStream file,
								@FormDataParam("file") final FormDataContentDisposition fileDisposition) {
		try {
			switch (getExtension(fileDisposition.getFileName())) {

				case "csv":
					// TODO: impl, would the need arise.
					assert false;
					try (final CSVReader reader = new CSVReader(new InputStreamReader(file))) {
						final List<String[]> list = reader.readAll();
						return Response.status(Status.OK).entity(list.toArray(new String[list.size()][])).build();
					}
				case "xls":
					try (final Workbook workbook = new HSSFWorkbook(file)) {
						processSheet(workbook.getSheetAt(pageNumber));
						return Response.status(Status.OK).build();
					}
				case "xlsx":
					try (final Workbook workbook = new XSSFWorkbook(file)) {
						processSheet(workbook.getSheetAt(pageNumber));
						return Response.status(Status.OK).build();
					}
				default:
					return Response.status(Status.BAD_REQUEST).entity("Uploaded file was neither a .xls nor a .xlsx file.").build();
			}
		} catch (final IOException e) {
			ImportEndpoint.LOGGER.error(String.format("Could not parse file '%s'.", fileDisposition.getFileName()), e);
			return Response.status(Status.BAD_REQUEST).entity(String.format("Could not parse file '%s'.", fileDisposition.getFileName())).build();
		}
	}

	private static String getExtension(final String fileName) {
		return fileName.substring(fileName.lastIndexOf(".") + 1);
	}

	private void processSheet(final Sheet sheet) {
		final Iterator<Row> rows = sheet.rowIterator();
		if (!rows.hasNext()) {
			return;
		}

		final Integer updt_pk = new Integer(this.ctx.nextval(Sequences.UPDATES_UPDT_PK_SEQ).intValue());
		this.ctx.insertInto(UPDATES).set(UPDATES.UPDT_PK, updt_pk).set(UPDATES.UPDT_DATE, new java.sql.Date(new Date().getTime())).execute();
		final Map<String, Integer> headersIndex = new HashMap<>();
		rows.next().cellIterator().forEachRemaining(cell -> headersIndex.put(readCell(cell, ""), Integer.valueOf(cell.getColumnIndex())));
		while (rows.hasNext()) {
			final Row row = rows.next();
			final Map<String, String> employee = new HashMap<>();
			headersIndex.forEach((header, idx) -> employee.put(header, readCell(row.getCell(idx.intValue()), header)));
			try {
				final String empl_pk = updateEmployee(employee);
				// TODO: parametrise site header's name
				this.ctx.insertInto(SITES_EMPLOYEES, SITES_EMPLOYEES.SIEM_UPDT_FK, SITES_EMPLOYEES.SIEM_SITE_FK, SITES_EMPLOYEES.SIEM_EMPL_FK)
						.values(updt_pk, employee.get("aurore brh"), empl_pk).execute();
			} catch (final Exception e) {
				// TODO: feedback
				// Ignoring non existing sites, non-parseable dates, empty
				// lines...
			}
		}

		// Set site #0 ('unassigned') to remaining employees
		this.ctx
				.select(EMPLOYEES.EMPL_PK)
				.from(EMPLOYEES)
				.where(
						EMPLOYEES.EMPL_PK.notIn(this.ctx.select(SITES_EMPLOYEES.SIEM_EMPL_FK).from(SITES_EMPLOYEES)
								.where(SITES_EMPLOYEES.SIEM_UPDT_FK.eq(updt_pk))))
				.fetch(EMPLOYEES.EMPL_PK)
				.forEach(
							empl_pk -> this.ctx.insertInto(
															SITES_EMPLOYEES,
															SITES_EMPLOYEES.SIEM_UPDT_FK,
															SITES_EMPLOYEES.SIEM_SITE_FK,
															SITES_EMPLOYEES.SIEM_EMPL_FK).values(updt_pk, "0", empl_pk).execute());
	}

	@SuppressWarnings("unchecked")
	// TODO: parametrise header's names
	private String updateEmployee(final Map<String, String> employee) throws ParseException {
		final String empl_pk = employee.get("Matricule GA");
		final Map<Object, Object> record = new HashMap<>();
		record.put(EMPLOYEES.EMPL_FIRSTNAME, employee.get("Prénom"));
		record.put(EMPLOYEES.EMPL_SURNAME, employee.get("Patronyme"));
		record.put(EMPLOYEES.EMPL_DOB, this.dateFormat.parseSql(employee.get("Date de naissance")));
		record.put(EMPLOYEES.EMPL_PERMANENT, Boolean.valueOf(employee.get("H-Nature du contrat ZAP")));
		if (this.ctx.fetchExists(EMPLOYEES, EMPLOYEES.EMPL_PK.eq(empl_pk))) {
			this.ctx.update(EMPLOYEES).set((Map<? extends Field<?>, ?>) record).where(EMPLOYEES.EMPL_PK.eq(empl_pk)).execute();
		} else {
			record.put(EMPLOYEES.EMPL_PK, empl_pk);
			this.ctx.insertInto(EMPLOYEES).set((Map<? extends Field<?>, ?>) record).execute();
		}

		return empl_pk;
	}

	// TODO: parametrise header's names
	private String readCell(final Cell cell, final String as) {
		if (cell == null) {
			return "";
		}

		switch (as) {
			case "Date de naissance":
				return this.dateFormat.format(DateUtil.getJavaDate(cell.getNumericCellValue()));

			case "H-Nature du contrat ZAP":
				return Boolean.valueOf(cell.getStringCellValue().equalsIgnoreCase("CDI")).toString();

			default:
				if (cell.getCellType() == Cell.CELL_TYPE_NUMERIC) {
					return String.valueOf(Double.valueOf(cell.getNumericCellValue()).intValue());
				}

				if (cell.getCellType() == Cell.CELL_TYPE_STRING) {
					return cell.getStringCellValue().trim();
				}

				return "";
		}
	}
}
