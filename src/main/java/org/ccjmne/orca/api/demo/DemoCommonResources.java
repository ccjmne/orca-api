package org.ccjmne.orca.api.demo;

import static org.ccjmne.orca.jooq.codegen.Tables.CERTIFICATES;
import static org.ccjmne.orca.jooq.codegen.Tables.CONFIGS;
import static org.ccjmne.orca.jooq.codegen.Tables.TRAINERPROFILES;
import static org.ccjmne.orca.jooq.codegen.Tables.TRAINERPROFILES_TRAININGTYPES;
import static org.ccjmne.orca.jooq.codegen.Tables.TRAININGTYPES;
import static org.ccjmne.orca.jooq.codegen.Tables.TRAININGTYPES_CERTIFICATES;
import static org.ccjmne.orca.jooq.codegen.Tables.TRAININGTYPES_DEFS;
import static org.ccjmne.orca.jooq.codegen.Tables.USERS_CERTIFICATES;

import java.util.Arrays;
import java.util.Collections;

import org.ccjmne.orca.api.utils.Constants;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.JSONB;
import org.jooq.impl.DSL;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;

public class DemoCommonResources {

	private static final Integer CERT_SST = Integer.valueOf(1);
	private static final Integer CERT_EPI = Integer.valueOf(2);
	private static final Integer CERT_DAE = Integer.valueOf(3);
	private static final Integer CERT_H0B0 = Integer.valueOf(4);
	private static final Integer CERT_EVAC = Integer.valueOf(5);
	private static final Integer CERT_FSST = Integer.valueOf(6);

	public static final Integer TRTY_SSTI = Integer.valueOf(1);
	public static final Integer TRTY_SSTR = Integer.valueOf(2);
	public static final Integer TRTY_EPI = Integer.valueOf(3);
	public static final Integer TRTY_DAE = Integer.valueOf(4);
	public static final Integer TRTY_H0B0 = Integer.valueOf(5);
	public static final Integer TRTY_EVAC = Integer.valueOf(6);
	public static final Integer TRTY_FSSTI = Integer.valueOf(7);
	public static final Integer TRTY_FSSTR = Integer.valueOf(8);

	private static final String TRAINERPROFILE_ALTERNATE = "Non-SST";

	public static void generate(final DSLContext ctx, final ObjectMapper mapper) {
		try {
			ctx.insertInto(CONFIGS, CONFIGS.CONF_TYPE, CONFIGS.CONF_NAME, CONFIGS.CONF_DATA)
					.values("pdf-site", "Tableau de Bord", JSONB.valueOf(mapper.writeValueAsString(ImmutableMap.<String, Object> builder()
							.put("fileName", "{{site}} - {{config}}")
							.put("size", "a4")
							.put("orientation", "landscape")
							.put("pages", Collections.singletonList(ImmutableMap.<String, Object> builder()
									.put("title", "Site de {{site}}")
									.put("subtitle", "au {{date}}")
									.put("bookmark", "Infos Sécurité")
									.put("lines", Arrays.asList(
																Collections.singletonList(ImmutableMap.<String, Object> builder()
																		.put("type", "dashboard")
																		.put("certificates", Arrays.asList(CERT_SST, CERT_FSST)).build()),
																Arrays.asList(
																				ImmutableMap.<String, Object> builder()
																						.put("type", "cert")
																						.put("cert", CERT_SST)
																						.put("columns", Integer.valueOf(2)).build(),
																				ImmutableMap.<String, Object> builder()
																						.put("type", "cert")
																						.put("cert", CERT_FSST)
																						.put("columns", Integer.valueOf(1)).build())))
									.build()))
							.build())))
					.execute();
		} catch (final JsonProcessingException e) {
			// Can not happen
			throw new RuntimeException(e);
		}

		ctx.insertInto(
						CERTIFICATES,
						CERTIFICATES.CERT_ORDER,
						CERTIFICATES.CERT_SHORT,
						CERTIFICATES.CERT_NAME,
						CERTIFICATES.CERT_TARGET,
						CERTIFICATES.CERT_PERMANENTONLY)
				.values(CERT_SST, "SST", "Sauveteur Secouriste du Travail", Integer.valueOf(15), Boolean.valueOf(false))
				.values(CERT_EPI, "EPI", "Équipement de Première Intervention", Integer.valueOf(66), Boolean.valueOf(false))
				.values(CERT_DAE, "DAE", "Sensibilisation Défibrillation", Integer.valueOf(30), Boolean.valueOf(false))
				.values(CERT_H0B0, "H0B0", "Habilitation Électrique", Integer.valueOf(20), Boolean.valueOf(false))
				.values(CERT_EVAC, "EVAC", "Agent d'Évacuation", Integer.valueOf(30), Boolean.valueOf(false))
				.values(CERT_FSST, "FSST", "Formateur SST", Integer.valueOf(0), Boolean.valueOf(false))
				.execute();

		// Set some relevant certificates
		ctx.insertInto(USERS_CERTIFICATES, USERS_CERTIFICATES.USCE_USER_FK, USERS_CERTIFICATES.USCE_CERT_FK)
				.values(Constants.USER_ROOT, CERT_SST)
				.values(Constants.USER_ROOT, CERT_H0B0)
				.values(Constants.USER_ROOT, CERT_EVAC)
				.execute();

		ctx.insertInto(
			TRAININGTYPES,
			TRAININGTYPES.TRTY_ORDER,
			TRAININGTYPES.TRTY_NAME)
				.values(TRTY_SSTI,  "SST Initiale")
				.values(TRTY_SSTR,  "Renouvellement SST")
				.values(TRTY_EPI,   "Manipulation Extincteurs")
				.values(TRTY_DAE,   "Sensibilisation Défibrillation")
				.values(TRTY_H0B0,  "Habilitation Électrique")
				.values(TRTY_EVAC,  "Agent d'Évacuation")
				.values(TRTY_FSSTI, "Formateur SST Initiale")
				.values(TRTY_FSSTR, "Renouvellement Formateur SST")
				.execute();

		ctx.insertInto(
			TRAININGTYPES_DEFS,
			TRAININGTYPES_DEFS.TTDF_TRTY_FK,
			TRAININGTYPES_DEFS.TTDF_PRESENCEONLY,
			TRAININGTYPES_DEFS.TTDF_EXTENDVALIDITY)
				.values(getType(TRTY_SSTI),  DSL.val(Boolean.FALSE), DSL.val(Boolean.FALSE))
				.values(getType(TRTY_SSTR),  DSL.val(Boolean.FALSE), DSL.val(Boolean.TRUE))
				.values(getType(TRTY_EPI),   DSL.val(Boolean.TRUE),  DSL.val(Boolean.FALSE))
				.values(getType(TRTY_DAE),   DSL.val(Boolean.TRUE),  DSL.val(Boolean.FALSE))
				.values(getType(TRTY_H0B0),  DSL.val(Boolean.FALSE), DSL.val(Boolean.FALSE))
				.values(getType(TRTY_EVAC),  DSL.val(Boolean.TRUE),  DSL.val(Boolean.FALSE))
				.values(getType(TRTY_FSSTI), DSL.val(Boolean.FALSE), DSL.val(Boolean.FALSE))
				.values(getType(TRTY_FSSTR), DSL.val(Boolean.FALSE), DSL.val(Boolean.FALSE))
				.execute();

		ctx.insertInto(
						TRAININGTYPES_CERTIFICATES,
						TRAININGTYPES_CERTIFICATES.TTCE_TTDF_FK,
						TRAININGTYPES_CERTIFICATES.TTCE_CERT_FK,
						TRAININGTYPES_CERTIFICATES.TTCE_DURATION)
				.values(FakeRecords.asFields(getTypeDef(TRTY_SSTI),  getCertificate(CERT_SST),  Integer.valueOf(12)))
				.values(FakeRecords.asFields(getTypeDef(TRTY_SSTI),  getCertificate(CERT_DAE),  Integer.valueOf(48)))

				.values(FakeRecords.asFields(getTypeDef(TRTY_SSTR),  getCertificate(CERT_SST),  Integer.valueOf(24)))
				.values(FakeRecords.asFields(getTypeDef(TRTY_SSTR),  getCertificate(CERT_DAE),  Integer.valueOf(48)))

				.values(FakeRecords.asFields(getTypeDef(TRTY_EPI),   getCertificate(CERT_EPI),  Integer.valueOf(36)))
				.values(FakeRecords.asFields(getTypeDef(TRTY_DAE),   getCertificate(CERT_DAE),  Integer.valueOf(24)))
				.values(FakeRecords.asFields(getTypeDef(TRTY_H0B0),  getCertificate(CERT_H0B0), Integer.valueOf(24)))
				.values(FakeRecords.asFields(getTypeDef(TRTY_EVAC),  getCertificate(CERT_EVAC), Integer.valueOf(12)))

				.values(FakeRecords.asFields(getTypeDef(TRTY_FSSTI), getCertificate(CERT_SST),  Integer.valueOf(12)))
				.values(FakeRecords.asFields(getTypeDef(TRTY_FSSTI), getCertificate(CERT_DAE),  Integer.valueOf(48)))
				.values(FakeRecords.asFields(getTypeDef(TRTY_FSSTI), getCertificate(CERT_FSST), Integer.valueOf(12)))

				.values(FakeRecords.asFields(getTypeDef(TRTY_FSSTR), getCertificate(CERT_SST),  Integer.valueOf(24)))
				.values(FakeRecords.asFields(getTypeDef(TRTY_FSSTR), getCertificate(CERT_DAE),  Integer.valueOf(0)))
				.values(FakeRecords.asFields(getTypeDef(TRTY_FSSTR), getCertificate(CERT_FSST), Integer.valueOf(24)))
				.execute();

		// From 2024-01-01, MAC FSST will *extend* the current validity of the certificates
		final Integer sstmac2 = ctx.insertInto(
			TRAININGTYPES_DEFS,
			TRAININGTYPES_DEFS.TTDF_TRTY_FK,
			TRAININGTYPES_DEFS.TTDF_EFFECTIVE_FROM,
			TRAININGTYPES_DEFS.TTDF_PRESENCEONLY,
			TRAININGTYPES_DEFS.TTDF_EXTENDVALIDITY)
				.values(getType(TRTY_FSSTR), Constants.fieldDate("2024-01-01"), DSL.val(Boolean.FALSE), DSL.val(Boolean.TRUE))
				.returning(TRAININGTYPES_DEFS.TTDF_PK).fetchOne().get(TRAININGTYPES_DEFS.TTDF_PK);
		ctx.insertInto(
						TRAININGTYPES_CERTIFICATES,
						TRAININGTYPES_CERTIFICATES.TTCE_TTDF_FK,
						TRAININGTYPES_CERTIFICATES.TTCE_CERT_FK,
						TRAININGTYPES_CERTIFICATES.TTCE_DURATION)
				.values(FakeRecords.asFields(sstmac2, getCertificate(CERT_SST),  Integer.valueOf(24)))
				.values(FakeRecords.asFields(sstmac2, getCertificate(CERT_DAE),  Integer.valueOf(0)))
				.values(FakeRecords.asFields(sstmac2, getCertificate(CERT_FSST), Integer.valueOf(24)))
				.execute();

		ctx.insertInto(TRAINERPROFILES, TRAINERPROFILES.TRPR_ID)
				.values(TRAINERPROFILE_ALTERNATE)
				.execute();

		ctx.insertInto(TRAINERPROFILES_TRAININGTYPES, TRAINERPROFILES_TRAININGTYPES.TPTT_TRPR_FK, TRAINERPROFILES_TRAININGTYPES.TPTT_TRTY_FK)
				.values(DSL.val(Constants.DEFAULT_TRAINERPROFILE), getType(TRTY_SSTI))
				.values(DSL.val(Constants.DEFAULT_TRAINERPROFILE), getType(TRTY_SSTR))
				.values(DSL.val(Constants.DEFAULT_TRAINERPROFILE), getType(TRTY_EPI))
				.values(DSL.val(Constants.DEFAULT_TRAINERPROFILE), getType(TRTY_DAE))
				.values(DSL.val(Constants.DEFAULT_TRAINERPROFILE), getType(TRTY_H0B0))
				.values(DSL.val(Constants.DEFAULT_TRAINERPROFILE), getType(TRTY_EVAC))
				.values(DSL.val(Constants.DEFAULT_TRAINERPROFILE), getType(TRTY_FSSTI))
				.values(DSL.val(Constants.DEFAULT_TRAINERPROFILE), getType(TRTY_FSSTR))

				.values(getTrainerProfile(TRAINERPROFILE_ALTERNATE), getType(TRTY_EPI))
				.values(getTrainerProfile(TRAINERPROFILE_ALTERNATE), getType(TRTY_DAE))
				.values(getTrainerProfile(TRAINERPROFILE_ALTERNATE), getType(TRTY_H0B0))
				.values(getTrainerProfile(TRAINERPROFILE_ALTERNATE), getType(TRTY_EVAC))
				.execute();
	}

	private static Field<Integer> getType(final Integer order) {
		return DSL.select(TRAININGTYPES.TRTY_PK).from(TRAININGTYPES).where(TRAININGTYPES.TRTY_ORDER.eq(order)).asField();
	}

	private static Field<Integer> getCertificate(final Integer order) {
		return DSL.select(CERTIFICATES.CERT_PK).from(CERTIFICATES).where(CERTIFICATES.CERT_ORDER.eq(order)).asField();
	}

	private static Field<Integer> getTrainerProfile(final String id) {
		return DSL.select(TRAINERPROFILES.TRPR_PK).from(TRAINERPROFILES).where(TRAINERPROFILES.TRPR_ID.eq(id)).asField();
	}

	private static Field<Integer> getTypeDef(final Integer order) {
		return DSL.select(TRAININGTYPES_DEFS.TTDF_PK)
			.from(TRAININGTYPES_DEFS)
			.join(TRAININGTYPES).on(TRAININGTYPES_DEFS.TTDF_TRTY_FK.eq(TRAININGTYPES.TRTY_PK))
			.where(TRAININGTYPES.TRTY_ORDER.eq(order)).asField();
	}
}
