package org.ccjmne.orca.api.rest;

import java.io.IOException;
import java.util.Map;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.github.jhonnymertz.wkhtmltopdf.wrapper.Pdf;
import com.github.jhonnymertz.wkhtmltopdf.wrapper.params.Param;

@Path("reports")
// TODO: Restrict access to authenticated users
public class ReportsEndpoint {

	@POST
	@Produces("application/pdf;charset=utf-8")
	@Consumes(MediaType.APPLICATION_JSON)
	public byte[] printReport(final Map<String, String> document) throws IOException, InterruptedException {
		final Pdf pdf = new Pdf();
		pdf.addParam(new Param("--disable-smart-shrinking"), new Param("--dpi", "96"), new Param("--zoom", ".8"), new Param("--encoding", "UTF-8"));
		pdf.addParam(new Param("--margin-top", "0"), new Param("--margin-right", "0"), new Param("--margin-bottom", "0"), new Param("--margin-left", "0"));
		pdf.addParam(new Param("--page-size", document.getOrDefault("size", "A4")));
		pdf.addParam(new Param("--orientation", document.getOrDefault("orientation", "Portrait")));

		pdf.addPageFromString(document.get("content"));
		return pdf.getPDF();
	}
}
