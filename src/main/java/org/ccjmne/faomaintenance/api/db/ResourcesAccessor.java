package org.ccjmne.faomaintenance.api.db;

import java.text.ParseException;
import java.util.List;

import org.ccjmne.faomaintenance.jooq.classes.tables.records.CertificatesRecord;
import org.ccjmne.faomaintenance.jooq.classes.tables.records.DepartmentsRecord;
import org.ccjmne.faomaintenance.jooq.classes.tables.records.TrainingtypesCertificatesRecord;
import org.ccjmne.faomaintenance.jooq.classes.tables.records.TrainingtypesRecord;
import org.jooq.Record;
import org.jooq.Result;

public interface ResourcesAccessor {

	public abstract Result<Record> listEmployees(String site_pk, String dateStr, String trng_pk) throws ParseException;

	public abstract Record lookupEmployee(String empl_pk);

	public abstract Result<Record> listSites(Integer dept_pk, String dateStr) throws ParseException;

	public abstract Record lookupSite(String site_pk);

	public abstract Result<Record> listTrainings(String empl_pk, List<Integer> types, String dateStr, String fromStr, String toStr) throws ParseException;

	public abstract Record lookupTraining(Integer trng_pk);

	public abstract Result<DepartmentsRecord> listDepartments();

	public abstract Result<TrainingtypesRecord> listTrainingTypes();

	public abstract Result<TrainingtypesCertificatesRecord> listTrainingTypesCertificates();

	public abstract Result<CertificatesRecord> listCertificates();

}