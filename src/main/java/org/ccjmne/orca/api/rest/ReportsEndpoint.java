package org.ccjmne.orca.api.rest;

import java.io.IOException;
import java.util.Map;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.github.jhonnymertz.wkhtmltopdf.wrapper.Pdf;

@Path("reports")
// TODO: Restrict access to authenticated users
public class ReportsEndpoint {

	@POST
	@Produces("application/pdf")
	@Consumes(MediaType.APPLICATION_JSON)
	public byte[] printReport(final Map<String, String> data) throws IOException, InterruptedException {
		final Pdf pdf = new Pdf();
		pdf.addPageFromString(data.get("content"));
		return pdf.getPDF();
	}
}
