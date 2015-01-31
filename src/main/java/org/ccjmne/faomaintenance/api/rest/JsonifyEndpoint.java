package org.ccjmne.faomaintenance.api.rest;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

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
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;

import au.com.bytecode.opencsv.CSVReader;

@Path("jsonify")
public class JsonifyEndpoint {

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
					try (final CSVReader reader = new CSVReader(new InputStreamReader(file))) {
						final List<String[]> list = reader.readAll();
						return Response.status(Status.OK).entity(list.toArray(new String[list.size()][])).build();
					}
				case "xls":
					try (final Workbook workbook = new HSSFWorkbook(file)) {
						return Response.status(Status.OK).entity(readSheetAt(workbook, pageNumber)).build();
					}
				case "xlsx":
					try (final Workbook workbook = new XSSFWorkbook(file)) {
						return Response.status(Status.OK).entity(readSheetAt(workbook, pageNumber)).build();
					}
				default:
					return Response.status(Status.BAD_REQUEST).entity("Uploaded file was neither a .xls nor a .xlsx file.").build();
			}
		} catch (final IOException e) {
			return Response.status(Status.BAD_REQUEST)
				.entity(String.format("Could not parse file '%s'.", fileDisposition.getFileName())).build();
		}
	}

	private static List<List<String>> readSheetAt(final Workbook workbook, final int index) {
		final Sheet sheet = workbook.getSheetAt(index);
		final List<List<String>> res = new ArrayList<>(sheet.getLastRowNum());
		int maxRowLength = index;
		for (final Row row : sheet) {
			final int rowLength = row.getLastCellNum();
			maxRowLength = Math.max(maxRowLength, rowLength);
			final List<String> cells = new ArrayList<>(maxRowLength);
			for (int i = 0; i < rowLength; i++) {
				final Cell cell = row.getCell(i, Row.RETURN_BLANK_AS_NULL);
				if (cell != null) {
					cells.add(cell.toString());
				} else {
					cells.add("");
				}
			}
			res.add(cells);
		}

		for (final List<String> row : res) {
			for (int i = row.size(); i < maxRowLength; i++) {
				row.add("");
			}
		}

		return res;
	}
}
