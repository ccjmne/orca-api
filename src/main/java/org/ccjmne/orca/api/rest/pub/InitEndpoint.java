package org.ccjmne.orca.api.rest.pub;

import static org.ccjmne.orca.jooq.codegen.Tables.USERS;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

import org.ccjmne.orca.api.demo.DemoBareWorkingState;
import org.ccjmne.orca.api.utils.Constants;
import org.ccjmne.orca.api.utils.Transactions;
import org.ccjmne.orca.api.utils.Transactions.TransactionConsumer;
import org.jooq.DSLContext;

import com.amazonaws.services.s3.AmazonS3Client;

@Path("init")
@Singleton
public class InitEndpoint {

  private static final String SECRET = System.getProperty("init.secret");

  private final DSLContext     ctx;
  private final AmazonS3Client client;

  @Inject
  public InitEndpoint(final DSLContext ctx, final AmazonS3Client client) {
    this.ctx = ctx;
    this.client = client;
  }

  @POST
  public void init(final String secret) {
    if ((SECRET == null) || SECRET.isEmpty() || !SECRET.equals(secret)) {
      throw new IllegalStateException("The instance is not set up for (re)initilisation or your password is invalid.");
    }

    if (null == this.ctx.selectFrom(USERS).where(USERS.USER_ID.eq(Constants.USER_ROOT)).fetchOne()) {
      throw new IllegalStateException("The database has already been initialised.");
    }

    Transactions.with(this.ctx, (TransactionConsumer<DSLContext>) transactionCtx -> DemoBareWorkingState.restore(transactionCtx, this.client));
  }
}
