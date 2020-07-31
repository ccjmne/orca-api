package org.ccjmne.orca.api.rest.utils;

import static org.ccjmne.orca.jooq.codegen.Tables.CONFIGS;

import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.ccjmne.orca.api.utils.Transactions;
import org.ccjmne.orca.jooq.codegen.Sequences;
import org.ccjmne.orca.jooq.codegen.tables.records.ConfigsRecord;
import org.jooq.DSLContext;
import org.jooq.JSONB;
import org.jooq.Result;
import org.jooq.SelectQuery;
import org.jooq.impl.DSL;

import com.fasterxml.jackson.databind.ObjectMapper;

@Path("configs")
public class ConfigurationsEndpoint {

  private static final List<String> AVAILABLE_TYPES = Arrays.asList("import-employees", "import-sites", "pdf-site");

  private final DSLContext ctx;

  @Inject
  public ConfigurationsEndpoint(final DSLContext ctx, final ObjectMapper mapper) {
    this.ctx = ctx;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Result<ConfigsRecord> listConfigs(@QueryParam("type") final String type) {
    if ((type != null) && !AVAILABLE_TYPES.contains(type)) {
      throw new IllegalArgumentException(String.format("Configuration can only be one of: %s", AVAILABLE_TYPES));
    }

    try (final SelectQuery<ConfigsRecord> query = DSL.selectFrom(CONFIGS).getQuery()) {
      query.addSelect(CONFIGS.CONF_PK, CONFIGS.CONF_TYPE, CONFIGS.CONF_NAME);
      if (type != null) {
        query.addConditions(CONFIGS.CONF_TYPE.eq(type));
      }

      return this.ctx.fetch(query);
    }
  }

  @GET
  @Path("{conf_pk}")
  @Produces(MediaType.APPLICATION_JSON)
  public ConfigsRecord lookupConfig(@PathParam("conf_pk") final Integer key) {
    return this.ctx.selectFrom(CONFIGS).where(CONFIGS.CONF_PK.eq(key)).fetchOne();
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public Integer createConfig(@QueryParam("type") final String type, @QueryParam("name") final String name, final JSONB config) {
    final Integer key = Integer.valueOf(this.ctx.nextval(Sequences.CONFIGS_CONF_PK_SEQ).intValue());
    this.updateConfig(key, type, name, config);
    return key;
  }

  @PUT
  @Path("{conf_pk}")
  @Consumes(MediaType.APPLICATION_JSON)
  public Boolean updateConfig(
                              @PathParam("conf_pk") final Integer key,
                              @QueryParam("type") final String type,
                              @QueryParam("name") final String name,
                              final JSONB config) {
    if ((type == null) || (name == null) || name.trim().isEmpty() || !AVAILABLE_TYPES.contains(type)) {
      throw new IllegalArgumentException(String.format("Configuration type and name must be provided, and type be one of: %s", AVAILABLE_TYPES));
    }

    return Transactions.with(this.ctx, transactionCtx -> {
      final String trimmed = name.trim();
      if (transactionCtx.fetchExists(CONFIGS, CONFIGS.CONF_TYPE.eq(type).and(CONFIGS.CONF_NAME.equalIgnoreCase(trimmed)).and(CONFIGS.CONF_PK.ne(key)))) {
        throw new IllegalArgumentException(String.format("A configuration item named '%s' already exists for the type '%s'.", trimmed, type));
      }

      final boolean exists = 1 == transactionCtx.deleteFrom(CONFIGS).where(CONFIGS.CONF_PK.eq(key)).execute();
      transactionCtx.insertInto(CONFIGS, CONFIGS.CONF_PK, CONFIGS.CONF_TYPE, CONFIGS.CONF_NAME, CONFIGS.CONF_DATA)
          .values(key, type, trimmed, config)
          .execute();

      return Boolean.valueOf(exists);
    });
  }

  @DELETE
  @Path("{conf_pk}")
  public boolean deleteConfig(@PathParam("conf_pk") final Integer key) {
    return 1 == this.ctx.deleteFrom(CONFIGS).where(CONFIGS.CONF_PK.eq(key)).execute();
  }
}
