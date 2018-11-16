package org.ccjmne.orca.api.rest.utils;

import static org.ccjmne.orca.jooq.classes.Tables.USERS;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;

import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.jooq.DSLContext;
import org.jooq.Record2;
import org.jooq.impl.DSL;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.github.zafarkhaja.semver.Version;

@Path("patch-notes")
public class PatchNotesEndpoint {

  public static class PatchNotes {

    // DTOs members are public final
    public final String version;
    public final long timestamp;
    public final String contents;

    private boolean unread;

    private PatchNotes(
                       @JsonProperty("version") final String version,
                       @JsonProperty("timestamp") final long timestamp,
                       @JsonProperty("contents") final String contents) {
      this.version = version;
      this.timestamp = timestamp;
      this.contents = contents;
    }

    @JsonProperty("unread")
    public boolean isUnread() {
      return this.unread;
    }

    void checkUnreadSince(final String lastCheckedVersion, final OffsetDateTime lastCheckedTimestamp) {
      this.unread = (null == lastCheckedVersion) || (null == lastCheckedTimestamp)
          || Version.valueOf(lastCheckedVersion).lessThan(Version.valueOf(this.version))
          || lastCheckedTimestamp.isBefore(OffsetDateTime.ofInstant(Instant.ofEpochMilli(this.timestamp), ZoneOffset.UTC));
    }
  }

  private static final String PATCH_NOTES_SERVICE_HOST = System.getProperty("patch_notes_service", "wfhqpe4fok.execute-api.eu-west-1.amazonaws.com");
  private static final String PATCH_NOTES_SERVICE_URL = String.format("https://%s/Prod?previous=true", PATCH_NOTES_SERVICE_HOST);

  private final DSLContext ctx;
  private final HttpClient client;
  private final ObjectMapper objectMapper;

  @Inject
  private PatchNotesEndpoint(final DSLContext ctx, final HttpClient client, final ObjectMapper objectMapper) {
    this.ctx = ctx;
    this.client = client;
    this.objectMapper = objectMapper;
  }

  @GET
  public List<PatchNotes> listRelevantPatchNotes(@QueryParam("version") final String version, @Context final HttpServletRequest request) throws Exception {
    final Record2<String, OffsetDateTime> lastChecked = this.ctx.select(USERS.USER_NEWSPULL_VERSION, USERS.USER_NEWSPULL_TIMESTAMP)
        .from(USERS).where(USERS.USER_ID.eq(request.getRemoteUser())).fetchOne();

    final List<PatchNotes> patches = this.client
        .execute(new HttpGet(new URIBuilder(PATCH_NOTES_SERVICE_URL).addParameter("version", version).build()),
                 response -> {
                   final int statusCode = response.getStatusLine().getStatusCode();
                   if ((statusCode >= 400) && (statusCode < 600)) {
                     try (final Scanner sc = new Scanner(response.getEntity().getContent())) {
                       throw new IllegalArgumentException(sc.useDelimiter("\\A").next());
                     }
                   }

                   return this.objectMapper.readValue(response.getEntity().getContent(),
                                                      TypeFactory.defaultInstance().constructCollectionType(ArrayList.class, PatchNotes.class));
                 });

    return patches.stream()
        .peek(patch -> patch.checkUnreadSince(lastChecked.get(USERS.USER_NEWSPULL_VERSION),
                                              lastChecked.get(USERS.USER_NEWSPULL_TIMESTAMP)))
        .collect(Collectors.toList());
  }

  @GET
  @Path("unread")
  public boolean hasUnread(@QueryParam("version") final String version, @Context final HttpServletRequest request) throws Exception {
    return listRelevantPatchNotes(version, request).stream().anyMatch(PatchNotes::isUnread);
  }

  @POST
  @Path("read")
  public void updateLastCheckedPatchNotes(@QueryParam("version") final String version, @Context final HttpServletRequest request) {
    try {
      Version.valueOf(version);
    } catch (final Exception e) {
      throw new IllegalArgumentException(String.format("Invalid version number: %s", version));
    }

    this.ctx.update(USERS)
        .set(USERS.USER_NEWSPULL_VERSION, version)
        .set(USERS.USER_NEWSPULL_TIMESTAMP, DSL.currentOffsetDateTime())
        .where(USERS.USER_ID.eq(request.getRemoteUser())).execute();
  }
}
