package org.ccjmne.orca.api.rest.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.pdfbox.io.MemoryUsageSetting;
import org.apache.pdfbox.multipdf.PDFMergerUtility;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.jhonnymertz.wkhtmltopdf.wrapper.Pdf;
import com.github.jhonnymertz.wkhtmltopdf.wrapper.params.Param;

@Path("reports")
public class ReportsEndpoint {

  private static final String DEFAULT_ORIENTATION = "portrait";
  private static final String DEFAULT_SIZE = "a4";

  public static class ReportRequest {

    private final String size;
    private final String orientation;
    private final List<String> pages;

    public ReportRequest(
                         @JsonProperty("size") final String size,
                         @JsonProperty("orientation") final String orientation,
                         @JsonProperty("pages") final List<String> pages) {
      this.size = size;
      this.orientation = orientation;
      this.pages = pages;
    }

    public String getSize() {
      if (this.size == null) {
        return DEFAULT_SIZE;
      }

      return this.size;
    }

    public String getOrientation() {
      if (this.orientation == null) {
        return DEFAULT_ORIENTATION;
      }

      return this.orientation;
    }

    public List<String> getPages() {
      if (this.pages == null) {
        return Collections.EMPTY_LIST;
      }

      return this.pages;
    }
  }

  @POST
  @Produces("application/pdf;charset=utf-8")
  @Consumes(MediaType.APPLICATION_JSON)
  public byte[] printReport(final ReportRequest document) throws IOException {
    final ByteArrayOutputStream res = new ByteArrayOutputStream();
    final PDFMergerUtility utility = new PDFMergerUtility();
    utility.setDestinationStream(res);
    utility.addSources(document.getPages().stream().map(page -> {
      final Pdf pdf = new Pdf();
      pdf.addParam(new Param("--disable-smart-shrinking"), new Param("--dpi", "96"), new Param("--zoom", ".78125"), new Param("--encoding", "UTF-8"));
      pdf.addParam(new Param("--margin-top", "0"), new Param("--margin-right", "0"), new Param("--margin-bottom", "0"), new Param("--margin-left", "0"));
      pdf.addParam(new Param("--page-size", document.getSize()));
      pdf.addParam(new Param("--orientation", document.getOrientation()));
      pdf.addPageFromString(page);
      try {
        return new ByteArrayInputStream(pdf.getPDF());
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    }).collect(Collectors.toList()));
    utility.mergeDocuments(MemoryUsageSetting.setupTempFileOnly());
    return res.toByteArray();
  }
}
