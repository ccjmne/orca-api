package org.ccjmne.faomaintenance.api.db.impl;

import static asdf.Tables.CERTIFICATES;
import static asdf.Tables.EMPLOYEES;
import static asdf.Tables.SITES;
import static asdf.Tables.SITES_EMPLOYEES;
import static asdf.Tables.TRAININGS;
import static asdf.Tables.TRAININGS_EMPLOYEES;
import static asdf.Tables.TRAININGTYPES;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.ccjmne.faomaintenance.api.db.DBClient;
import org.ccjmne.faomaintenance.api.resources.Certificate;
import org.ccjmne.faomaintenance.api.resources.Employee;
import org.ccjmne.faomaintenance.api.resources.Site;
import org.ccjmne.faomaintenance.api.resources.Training;
import org.ccjmne.faomaintenance.api.resources.Training.Type;
import org.ccjmne.faomaintenance.api.resources.Update;
import org.ccjmne.faomaintenance.api.utils.NamedParameterStatement;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.SQLDialect;
import org.jooq.SelectQuery;
import org.jooq.impl.DSL;

import asdf.Sequences;

public class PostgresDBClientImpl implements DBClient {

	private static final String DB_USER = System.getProperty("db_user", "postgres");
	private static final String DB_PASS = System.getProperty("db_pass", "asdf123");
	private static final String DB_URL = String.format(
														"jdbc:postgresql://%s:%s/%s",
														System.getProperty("db_host", "localhost"),
														System.getProperty("db_port", "5432"),
														System.getProperty("db_name", "postgres"));

	private final DateFormat dateFormat;

	private final Connection connection;

	private final NamedParameterStatement updateEmployeeNamedStatement;
	private final NamedParameterStatement insertEmployeeNamedStatement;

	private final PreparedStatement lookupSiteStatement;
	private final PreparedStatement listSitesStatement;

	private final PreparedStatement listCertificatesStatement;
	private final PreparedStatement listTrainingTypesStatement;

	private final DSLContext ctx;

	@Inject
	public PostgresDBClientImpl(final DateFormat dateFormat) throws SQLException, ClassNotFoundException {
		Class.forName("org.postgresql.Driver");
		this.dateFormat = dateFormat;
		this.connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
		this.ctx = DSL.using(DriverManager.getConnection(DB_URL, DB_USER, DB_PASS), SQLDialect.POSTGRES);
		this.insertEmployeeNamedStatement = new NamedParameterStatement(
																		this.connection,
																		"INSERT INTO employees (empl_pk, empl_firstname, empl_surname, empl_dob, empl_permanent) VALUES (:empl_pk, :empl_firstname, :empl_surname, date(:empl_dob), :empl_permanent)");
		this.updateEmployeeNamedStatement = new NamedParameterStatement(
																		this.connection,
																		"UPDATE employees SET empl_firstname = :empl_firstname, empl_surname = :empl_surname, empl_dob = date(:empl_dob), empl_permanent = :empl_permanent WHERE empl_pk = :empl_pk");

		this.lookupSiteStatement = this.connection.prepareStatement("SELECT * FROM sites WHERE site_pk = ?");
		this.listSitesStatement = this.connection.prepareStatement("SELECT * FROM sites");

		this.listTrainingTypesStatement = this.connection.prepareStatement("SELECT * FROM trainingtypes");
		this.listCertificatesStatement = this.connection.prepareStatement("SELECT * FROM certificates");
	}

	public List<Certificate> listCertificates() {
		final List<Certificate> res = new ArrayList<>();
		try (final ResultSet resultSet =
				this.listCertificatesStatement.executeQuery()) {
			while (resultSet.next()) {
				res.add(asCertificate(resultSet));
			}
		} catch (final SQLException e) {
			// TODO: implement?
			e.printStackTrace();
		}

		return res;
	}

	public List<Training.Type> listTrainingTypes() {
		final List<Training.Type> res = new ArrayList<>();
		try (final ResultSet resultSet =
				this.listTrainingTypesStatement.executeQuery()) {
			while (resultSet.next()) {
				res.add(asTrainingType(resultSet));
			}
		} catch (final SQLException e) {
			// TODO: implement?
			e.printStackTrace();
		}

		return res;
	}

	@Override
	public List<Site> listSites() {
		final List<Site> res = new ArrayList<>();
		try (final ResultSet resultSet = this.listSitesStatement.executeQuery()) {
			while (resultSet.next()) {
				res.add(asSite(resultSet));
			}
		} catch (final SQLException e) {
			// TODO: implement?
			e.printStackTrace();
		}

		return res;
	}

	@Override
	public Site lookupSite(final String aurore) {
		try {
			this.lookupSiteStatement.setString(1, aurore);
			try (final ResultSet resultSet = this.lookupSiteStatement.executeQuery()) {
				if (!resultSet.next()) {
					return null;
				}

				return asSite(resultSet);
			}
		} catch (final SQLException e) {
			// TODO: implement?
			e.printStackTrace();
			return null;
		}
	}

	private Site asSite(final ResultSet resultSet) throws SQLException {
		final Site res = new Site();
		res.aurore = resultSet.getString("site_pk");
		res.name = resultSet.getString("site_name");
		res.employees = listEmployees(res.aurore);
		return res;
	}

	@Override
	public Employee lookupEmployee(final String registrationNumber) {
		return asEmployee(this.ctx.select().from(EMPLOYEES).where(EMPLOYEES.EMPL_PK.equal(registrationNumber)).fetchOne());
	}

	private Employee asEmployee(final Record r) {
		return new Employee(
							r.getValue(EMPLOYEES.EMPL_PK),
							r.getValue(EMPLOYEES.EMPL_FIRSTNAME),
							r.getValue(EMPLOYEES.EMPL_SURNAME),
							new java.util.Date(r.getValue(EMPLOYEES.EMPL_DOB).getTime()),
							r.getValue(EMPLOYEES.EMPL_PERMANENT).booleanValue(),
							listEmployeeTrainings(r.getValue(EMPLOYEES.EMPL_PK)));
	}

	private static Training asTraining(final ResultSet resultSet) throws SQLException {
		return new Training(resultSet.getInt("trng_pk"), asTrainingType(resultSet), resultSet.getDate("trng_date"));
	}

	private static Training.Type asTrainingType(final ResultSet resultSet) throws SQLException {
		return new Type(resultSet.getInt("trty_pk"), resultSet.getInt("trty_validity"), asCertificate(resultSet), resultSet.getString("trty_name"));
	}

	private static Certificate asCertificate(final ResultSet resultSet) throws SQLException {
		return new Certificate(resultSet.getInt("cert_target"), resultSet.getBoolean("cert_permanentonly"), resultSet.getString("cert_name"));
	}

	@Override
	public boolean addEmployee(final Employee employee) {
		try {
			this.insertEmployeeNamedStatement.setString("empl_pk", employee.getRegistrationNumber());
			this.insertEmployeeNamedStatement.setString("empl_firstname", employee.getFirstName());
			this.insertEmployeeNamedStatement.setString("empl_surname", employee.getSurname());
			this.insertEmployeeNamedStatement.setDate("empl_dob", employee.getDateOfBirth());
			this.insertEmployeeNamedStatement.setBoolean("empl_permanent", employee.isPermanent());
			this.insertEmployeeNamedStatement.executeUpdate();
			return true;
		} catch (final SQLException e) {
			e.printStackTrace();
			return false;
		}
	}

	@Override
	public boolean updateEmployee(final String registrationNumber, final Employee employee) {
		try {
			this.updateEmployeeNamedStatement.setString("empl_pk", registrationNumber);
			this.updateEmployeeNamedStatement.setString("empl_firstname", employee.getFirstName());
			this.updateEmployeeNamedStatement.setString("empl_surname", employee.getSurname());
			this.updateEmployeeNamedStatement.setDate("empl_dob", employee.getDateOfBirth());
			this.updateEmployeeNamedStatement.setBoolean("empl_permanent", employee.isPermanent());
			this.updateEmployeeNamedStatement.executeUpdate();
			return true;
		} catch (final SQLException e) {
			e.printStackTrace();
			return false;
		}
	}

	@Override
	public void registerUpdate(final Update update) {
		try (final Statement statement = this.connection.createStatement()) {
			statement.executeUpdate(
									"INSERT INTO updates (updt_date) VALUES ('" + this.dateFormat.format(update.getDate()) + "')",
									Statement.RETURN_GENERATED_KEYS);
			final int update_pk;
			try (final ResultSet generatedKeys = statement.getGeneratedKeys()) {
				generatedKeys.next();
				update_pk = generatedKeys.getInt("updt_pk");
			}

			// updating all employees found
			for (final Entry<Employee, Site> assignment : update.getAssignmentMap().entrySet()) {
				final Employee employee = assignment.getKey();
				final Site site = assignment.getValue();
				if (lookupEmployee(employee.getRegistrationNumber()) == null) {
					addEmployee(employee);
				} else {
					updateEmployee(employee.getRegistrationNumber(), employee);
				}

				statement.executeUpdate("INSERT INTO sites_employees (siem_updt_fk, siem_site_fk, siem_empl_fk) VALUES ('" + update_pk + "', '"
						+ site.aurore + "', '" + employee.getRegistrationNumber() + "')");
			}

			// setting "unassigned" site #0 to missing employees
			statement.executeUpdate(
					"INSERT INTO sites_employees\n" +
							"	(siem_site_fk)\n" +
							"	SELECT \n" +
							"		'" + update_pk + "', empl_pk, '0'\n" +
							"	FROM \n" +
							"		employees\n" +
							"	WHERE\n" +
							"		employees.empl_pk NOT IN\n" +
							"			(SELECT \n" +
							"				sites_employees.siem_empl_fk\n" +
							"			FROM\n" +
							"				sites_employees\n" +
							"			WHERE\n" +
							"				sites_employees.siem_updt_fk = '" + update_pk + "');");
		} catch (final SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public List<Employee> listEmployees(final String aurore) {
		return this.ctx
				.select(EMPLOYEES.EMPL_PK)
				.from(EMPLOYEES, SITES, SITES_EMPLOYEES)
				.where(
				       EMPLOYEES.EMPL_PK.equal(SITES_EMPLOYEES.SIEM_EMPL_FK).and(SITES_EMPLOYEES.SIEM_SITE_FK.equal(SITES.SITE_PK))
				       .and(SITES.SITE_PK.equal(aurore))).fetch().map(r -> lookupEmployee(r.getValue(EMPLOYEES.EMPL_PK)));
	}

	@Override
	public List<Training> listTrainings(final Date from, final Integer... types) {
		return this.listTrainings(from, null, types);
	}

	@Override
	public List<Training> listTrainings(final Date from, final Date to, final Integer... types) {
		final SelectQuery<Record> query = this.ctx.selectQuery();
		query.addFrom(TRAININGS, TRAININGTYPES, CERTIFICATES);
		query.addConditions(TRAININGS.TRNG_DATE.ge(new java.sql.Date(from.getTime())).and(TRAININGS.TRNG_TRTY_FK.equal(TRAININGTYPES.TRTY_PK))
		                    .and(TRAININGTYPES.TRTY_CERT_FK.equal(CERTIFICATES.CERT_PK)));
		if (to != null) {
			query.addConditions(TRAININGS.TRNG_DATE.le(new java.sql.Date(to.getTime())));
		}
		if (types.length > 0) {
			query.addConditions(TRAININGTYPES.TRTY_PK.in(types));
		}
		return query.fetch().stream().map(r -> asTraining(r)).collect(Collectors.toList());
	}

	private Map<Training, Boolean> listEmployeeTrainings(final String employeeId) {
		return this.ctx
				.select()
				.from(TRAININGS, TRAININGTYPES, CERTIFICATES, TRAININGS_EMPLOYEES)
				.where(
						TRAININGS_EMPLOYEES.TREM_EMPL_FK.equal(employeeId)
				       .and(TRAININGS_EMPLOYEES.TREM_TRNG_FK.equal(TRAININGS.TRNG_PK)
										.and(TRAININGS.TRNG_TRTY_FK.equal(TRAININGTYPES.TRTY_PK)
												.and(TRAININGTYPES.TRTY_CERT_FK.equal(CERTIFICATES.CERT_PK)))))
				.fetch().stream().collect(Collectors.toMap(r -> asTraining(r), r -> r.getValue(TRAININGS_EMPLOYEES.TREM_VALID)));
	}

	@Override
	public int addTraining(final Map<String, Object> map) {
		return insertTraining(this.ctx.nextval(Sequences.TRAININGS_TRNG_PK_SEQ).intValue(), map);
	}

	@SuppressWarnings("unchecked")
	private int insertTraining(final int trainingId, final Map<String, Object> map) {
		try {
			this.ctx
					.insertInto(TRAININGS, TRAININGS.TRNG_PK, TRAININGS.TRNG_TRTY_FK, TRAININGS.TRNG_DATE)
					.values(
							Integer.valueOf(trainingId),
							(Integer) map.get("type"),
							new java.sql.Date(this.dateFormat.parse(map.get("date").toString()).getTime())).execute();
		} catch (final ParseException e) {
			// swallow?
			e.printStackTrace();
		}
		((Map<String, Map<String, String>>) map.getOrDefault("trainees", Collections.EMPTY_MAP)).forEach((id, m) -> this.ctx
				.insertInto(
							TRAININGS_EMPLOYEES,
							TRAININGS_EMPLOYEES.TREM_TRNG_FK,
							TRAININGS_EMPLOYEES.TREM_EMPL_FK,
							TRAININGS_EMPLOYEES.TREM_VALID,
							TRAININGS_EMPLOYEES.TREM_COMMENT)
				.values(Integer.valueOf(trainingId), id, Boolean.valueOf(String.valueOf(m.get("valid")).equalsIgnoreCase("true")), m.get("comment")).execute());
		return trainingId;
	}

	@Override
	public boolean updateTraining(final int trainingId, final Map<String, Object> map) {
		final boolean exists = deleteTraining(trainingId);
		insertTraining(trainingId, map);
		return exists;
	}

	@Override
	public boolean deleteTraining(final int trainingId) {
		final boolean exists = this.ctx.select().from(TRAININGS).where(TRAININGS.TRNG_PK.equal(Integer.valueOf(trainingId))).fetch().isNotEmpty();
		if (exists) {
			this.ctx.delete(TRAININGS).where(TRAININGS.TRNG_PK.equal(Integer.valueOf(trainingId))).execute();
		}

		return exists;
	}

	private static Training asTraining(final Record r) {
		if (r == null) {
			return null;
		}

		return new Training(r.getValue(TRAININGS.TRNG_PK).intValue(), asTrainingType(r), r.getValue(TRAININGS.TRNG_DATE));
	}

	@SuppressWarnings("boxing")
	private static Type asTrainingType(final Record r) {
		return new Training.Type(
									r.getValue(TRAININGTYPES.TRTY_PK),
									r.getValue(TRAININGTYPES.TRTY_VALIDITY),
									asCertificate(r),
									r.getValue(TRAININGTYPES.TRTY_NAME));
	}

	@SuppressWarnings("boxing")
	private static Certificate asCertificate(final Record r) {
		return new Certificate(r.getValue(CERTIFICATES.CERT_TARGET), r.getValue(CERTIFICATES.CERT_PERMANENTONLY), r.getValue(CERTIFICATES.CERT_NAME));
	}

	@Override
	public Training lookupTraining(final int trainingId) {
		return asTraining(this.ctx
				.select()
				.from(TRAININGS, TRAININGTYPES, CERTIFICATES)
				.where(
						TRAININGS.TRNG_PK.equal(Integer.valueOf(trainingId)).and(TRAININGTYPES.TRTY_PK.equal(TRAININGS.TRNG_TRTY_FK))
								.and(CERTIFICATES.CERT_PK.equal(TRAININGTYPES.TRTY_CERT_FK))).fetchOne());
	}
}