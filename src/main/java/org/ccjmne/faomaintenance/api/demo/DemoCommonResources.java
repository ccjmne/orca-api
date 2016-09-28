package org.ccjmne.faomaintenance.api.demo;

import static org.ccjmne.faomaintenance.jooq.classes.Tables.CERTIFICATES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAINERPROFILES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAINERPROFILES_TRAININGTYPES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGTYPES;
import static org.ccjmne.faomaintenance.jooq.classes.Tables.TRAININGTYPES_CERTIFICATES;

import org.ccjmne.faomaintenance.api.utils.Constants;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.impl.DSL;

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

	public static void generate(final DSLContext ctx) {
		ctx.insertInto(
						CERTIFICATES,
						CERTIFICATES.CERT_ORDER,
						CERTIFICATES.CERT_SHORT,
						CERTIFICATES.CERT_NAME,
						CERTIFICATES.CERT_TARGET,
						CERTIFICATES.CERT_PERMANENTONLY)
				.values(CERT_SST, "SST", "Sauveteur Secouriste du Travail", Integer.valueOf(10), Boolean.valueOf(false))
				.values(CERT_EPI, "EPI", "Équipement de Première Intervention", Integer.valueOf(66), Boolean.valueOf(false))
				.values(CERT_DAE, "DAE", "Sensibilisation Défibrillation", Integer.valueOf(50), Boolean.valueOf(false))
				.values(CERT_H0B0, "H0B0", "Habilitation Électrique", Integer.valueOf(15), Boolean.valueOf(false))
				.values(CERT_EVAC, "EVAC", "Agent d'Évacuation", Integer.valueOf(30), Boolean.valueOf(false))
				.values(CERT_FSST, "FSST", "Formateur SST", Integer.valueOf(0), Boolean.valueOf(false))
				.execute();

		ctx.insertInto(TRAININGTYPES, TRAININGTYPES.TRTY_ORDER, TRAININGTYPES.TRTY_NAME, TRAININGTYPES.TRTY_VALIDITY)
				.values(TRTY_SSTI, "SST Initiale", Integer.valueOf(36))
				.values(TRTY_SSTR, "Renouvellement SST", Integer.valueOf(24))
				.values(TRTY_EPI, "Manipulation Extincteurs", Integer.valueOf(36))
				.values(TRTY_DAE, "Sensibilisation Défibrillation", Integer.valueOf(24))
				.values(TRTY_H0B0, "Habilitation Électrique", Integer.valueOf(60))
				.values(TRTY_EVAC, "Agent d'Évacuation", Integer.valueOf(60))
				.values(TRTY_FSSTI, "Formateur SST Initiale", Integer.valueOf(48))
				.values(TRTY_FSSTR, "Renouvellement Formateur SST", Integer.valueOf(36))
				.execute();

		ctx.insertInto(TRAININGTYPES_CERTIFICATES, TRAININGTYPES_CERTIFICATES.TTCE_TRTY_FK, TRAININGTYPES_CERTIFICATES.TTCE_CERT_FK)
				.values(getTrainingType(TRTY_SSTI), getCertificate(CERT_SST))
				.values(getTrainingType(TRTY_SSTI), getCertificate(CERT_DAE))

				.values(getTrainingType(TRTY_SSTR), getCertificate(CERT_SST))
				.values(getTrainingType(TRTY_SSTR), getCertificate(CERT_DAE))

				.values(getTrainingType(TRTY_EPI), getCertificate(CERT_EPI))

				.values(getTrainingType(TRTY_DAE), getCertificate(CERT_DAE))

				.values(getTrainingType(TRTY_H0B0), getCertificate(CERT_H0B0))

				.values(getTrainingType(TRTY_EVAC), getCertificate(CERT_EVAC))

				.values(getTrainingType(TRTY_FSSTI), getCertificate(CERT_SST))
				.values(getTrainingType(TRTY_FSSTI), getCertificate(CERT_DAE))
				.values(getTrainingType(TRTY_FSSTI), getCertificate(CERT_FSST))

				.values(getTrainingType(TRTY_FSSTR), getCertificate(CERT_SST))
				.values(getTrainingType(TRTY_FSSTR), getCertificate(CERT_DAE))
				.values(getTrainingType(TRTY_FSSTR), getCertificate(CERT_FSST))
				.execute();

		ctx.insertInto(TRAINERPROFILES, TRAINERPROFILES.TRPR_ID)
				.values(TRAINERPROFILE_ALTERNATE)
				.execute();

		ctx.insertInto(TRAINERPROFILES_TRAININGTYPES, TRAINERPROFILES_TRAININGTYPES.TPTT_TRPR_FK, TRAINERPROFILES_TRAININGTYPES.TPTT_TRTY_FK)
				.values(DSL.val(Constants.UNASSIGNED_TRAINERPROFILE), getTrainingType(TRTY_SSTI))
				.values(DSL.val(Constants.UNASSIGNED_TRAINERPROFILE), getTrainingType(TRTY_SSTR))
				.values(DSL.val(Constants.UNASSIGNED_TRAINERPROFILE), getTrainingType(TRTY_EPI))
				.values(DSL.val(Constants.UNASSIGNED_TRAINERPROFILE), getTrainingType(TRTY_DAE))
				.values(DSL.val(Constants.UNASSIGNED_TRAINERPROFILE), getTrainingType(TRTY_H0B0))
				.values(DSL.val(Constants.UNASSIGNED_TRAINERPROFILE), getTrainingType(TRTY_EVAC))
				.values(DSL.val(Constants.UNASSIGNED_TRAINERPROFILE), getTrainingType(TRTY_FSSTI))
				.values(DSL.val(Constants.UNASSIGNED_TRAINERPROFILE), getTrainingType(TRTY_FSSTR))

				.values(getTrainerProfile(TRAINERPROFILE_ALTERNATE), getTrainingType(TRTY_EPI))
				.values(getTrainerProfile(TRAINERPROFILE_ALTERNATE), getTrainingType(TRTY_DAE))
				.values(getTrainerProfile(TRAINERPROFILE_ALTERNATE), getTrainingType(TRTY_H0B0))
				.values(getTrainerProfile(TRAINERPROFILE_ALTERNATE), getTrainingType(TRTY_EVAC))
				.execute();
	}

	private static Field<Integer> getTrainingType(final Integer order) {
		return DSL.select(TRAININGTYPES.TRTY_PK).from(TRAININGTYPES).where(TRAININGTYPES.TRTY_ORDER.eq(order)).asField();
	}

	private static Field<Integer> getCertificate(final Integer order) {
		return DSL.select(CERTIFICATES.CERT_PK).from(CERTIFICATES).where(CERTIFICATES.CERT_ORDER.eq(order)).asField();
	}

	private static Field<Integer> getTrainerProfile(final String id) {
		return DSL.select(TRAINERPROFILES.TRPR_PK).from(TRAINERPROFILES).where(TRAINERPROFILES.TRPR_ID.eq(id)).asField();
	}
}
