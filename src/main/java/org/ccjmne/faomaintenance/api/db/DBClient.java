package org.ccjmne.faomaintenance.api.db;

import java.util.Date;
import java.util.List;
import java.util.Map;

import org.ccjmne.faomaintenance.api.resources.Employee;
import org.ccjmne.faomaintenance.api.resources.Site;
import org.ccjmne.faomaintenance.api.resources.Training;
import org.ccjmne.faomaintenance.api.resources.Update;

public interface DBClient {

	public abstract void registerUpdate(final Update update);

	public abstract List<Site> listSites();

	public abstract Site lookupSite(final String aurore);

	public abstract List<Employee> listEmployees(final String aurore);

	public abstract Employee lookupEmployee(final String registrationNumber);

	/**
	 * Inserts a <b>new</b> employee in the database.
	 *
	 * @param employee
	 *            the employee to create
	 * @return <b>true</b> if the employee could be created, <b>false</b>
	 *         otherwise
	 */
	public abstract boolean addEmployee(final Employee employee);

	/**
	 * Updates an <b>existing</b> employee in the database.
	 *
	 * @param registrationNumber
	 *            the employee's registration number
	 * @param employee
	 *            the employee to create
	 * @return <b>true</b> if the employee could be overridden, <b>false</b>
	 *         otherwise
	 */
	public abstract boolean updateEmployee(final String registrationNumber, final Employee employee);

	public abstract Training lookupTraining(final int trainingId);

	/**
	 *
	 * @param from
	 *            inclusive.
	 * @param to
	 *            inclusive.
	 * @param types
	 *            if empty, no filtering
	 * @return
	 */
	public abstract List<Training> listTrainings(final Date from, final Date to, final Integer... types);

	/**
	 *
	 * @param from
	 *            inclusive.
	 * @param types
	 *            if empty, no filtering
	 * @return
	 */
	public abstract List<Training> listTrainings(final Date from, final Integer... types);

	public abstract int addTraining(final Map<String, Object> map);

	public abstract boolean updateTraining(final int trainingId, final Map<String, Object> map);

	public abstract boolean deleteTraining(final int trainingId);

}