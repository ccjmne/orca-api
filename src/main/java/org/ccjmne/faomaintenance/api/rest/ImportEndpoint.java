package org.ccjmne.faomaintenance.api.rest;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.List;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.commons.io.FilenameUtils;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.ccjmne.faomaintenance.api.db.DBClient;
import org.ccjmne.faomaintenance.api.resources.Employee;
import org.ccjmne.faomaintenance.api.resources.Site;
import org.ccjmne.faomaintenance.api.resources.Update;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.jboss.logging.Logger;

import au.com.bytecode.opencsv.CSVReader;

@Path("update")
public class ImportEndpoint {

	private final DBClient dbClient;
	private static final Logger LOGGER = Logger.getLogger(ImportEndpoint.class);

	@Inject
	public ImportEndpoint(final DBClient dbClient) {
		this.dbClient = dbClient;
	}

	@POST
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Produces(MediaType.APPLICATION_JSON)
	public Response postForm(
	                         @QueryParam("page") final int pageNumber,
	                         @FormDataParam("file") final InputStream file,
	                         @FormDataParam("file") final FormDataContentDisposition fileDisposition) {
		try {
			switch (FilenameUtils.getExtension(fileDisposition.getFileName())) {
				case "csv":
					assert false;
					try (final CSVReader reader = new CSVReader(new InputStreamReader(file))) {
						final List<String[]> list = reader.readAll();
						return Response.status(Status.OK).entity(list.toArray(new String[list.size()][])).build();
					}
				case "xls":
					try (final Workbook workbook = new HSSFWorkbook(file)) {
						readSheetAt(workbook, pageNumber);
						return Response.status(Status.OK).build();
					}
				case "xlsx":
					try (final Workbook workbook = new XSSFWorkbook(file)) {
						readSheetAt(workbook, pageNumber);
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

	private void readSheetAt(final Workbook workbook, final int index) {
		final Iterator<Row> rows = workbook.getSheetAt(index).rowIterator();
		if (rows.hasNext()) {
			// Skip the headers row:
			rows.next();
		}

		final Update update = new Update();
		while (rows.hasNext()) {
			populateUpdate(rows, update);
		}
		this.dbClient.registerUpdate(update);
	}

	private void populateUpdate(final Iterator<Row> rows, final Update update) {
		final Employee employee = new Employee();
		for (final Cell cell : rows.next()) {
			switch (cell.getColumnIndex()) {
				case 0:
					// Public or private
					break;

				case 1:
					employee.registrationNumber = cell.getStringCellValue();
					break;

				case 2:
					employee.surname = cell.getStringCellValue();
					break;

				case 3:
					employee.firstName = cell.getStringCellValue();
					break;

				case 4:
					employee.dateOfBirth = DateUtil.getJavaDate(cell.getNumericCellValue());
					break;

				case 5:
					employee.permanent = cell.getStringCellValue().equals("CDI");
					break;

				case 7:
					// Site's Aurore code
					final String aurore;
					if (cell.getCellType() == Cell.CELL_TYPE_NUMERIC) {
						aurore = String.valueOf(Double.valueOf(cell.getNumericCellValue()).intValue());
					} else {
						aurore = cell.getStringCellValue();
					}

					final Site site;
					if (aurore.isEmpty()) {
						site = this.dbClient.lookupSite("0");
					} else {
						site = this.dbClient.lookupSite(aurore);
					}

					if (site != null) {
						update.assign(employee, site);
					}
					break;

				case 9:
					employee.jobTitle = cell.getStringCellValue();
					break;

				default:
					break;
			}
		}
	}
}
