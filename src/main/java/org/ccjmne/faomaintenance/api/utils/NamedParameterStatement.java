package org.ccjmne.faomaintenance.api.utils;

import java.sql.Array;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NamedParameterStatement {

	private final PreparedStatement statement;
	private final Map<String, List<Integer>> indexMap;

	public NamedParameterStatement(final Connection connection, final String query) throws SQLException {
		this.indexMap = new HashMap<>();
		final String parsedQuery = parse(query, this.indexMap);
		this.statement = connection.prepareStatement(parsedQuery);
	}

	static final String parse(final String query, final Map<String, List<Integer>> paramMap) {
		final int length = query.length();
		final StringBuffer parsedQuery = new StringBuffer(length);
		boolean inSingleQuote = false;
		boolean inDoubleQuote = false;
		int index = 1;

		for (int i = 0; i < length; i++) {
			char c = query.charAt(i);
			if (inSingleQuote) {
				if (c == '\'') {
					inSingleQuote = false;
				}
			} else if (inDoubleQuote) {
				if (c == '"') {
					inDoubleQuote = false;
				}
			} else {
				if (c == '\'') {
					inSingleQuote = true;
				} else if (c == '"') {
					inDoubleQuote = true;
				} else if ((c == ':') && ((i + 1) < length) &&
						Character.isJavaIdentifierStart(query.charAt(i + 1))) {
					int j = i + 2;
					while ((j < length) && Character.isJavaIdentifierPart(query.charAt(j))) {
						j++;
					}
					final String name = query.substring(i + 1, j);
					c = '?'; // replace the parameter with a question mark
					i += name.length(); // skip past the end if the parameter

					List<Integer> indexList = paramMap.get(name);
					if (indexList == null) {
						indexList = new ArrayList<>();
						paramMap.put(name, indexList);
					}

					indexList.add(new Integer(index++));
				}
			}

			parsedQuery.append(c);
		}

		return parsedQuery.toString();
	}

	private int[] getIndexes(final String name) {
		final List<Integer> indexes = this.indexMap.get(name);
		if (indexes == null) {
			throw new IllegalArgumentException("Parameter not found: " + name);
		}

		final int[] ints = new int[indexes.size()];
		for (int i = 0; i < indexes.size(); i++) {
			ints[i] = indexes.get(i).intValue();
		}

		return ints;
	}

	public void setObject(final String name, final Object value) throws SQLException {
		final int[] indexes = getIndexes(name);
		for (final int indexe : indexes) {
			this.statement.setObject(indexe, value);
		}
	}

	public void setString(final String name, final String value) throws SQLException {
		final int[] indexes = getIndexes(name);
		for (final int indexe : indexes) {
			this.statement.setString(indexe, value);
		}
	}

	public void setInt(final String name, final int value) throws SQLException {
		final int[] indexes = getIndexes(name);
		for (final int indexe : indexes) {
			this.statement.setInt(indexe, value);
		}
	}

	public void setLong(final String name, final long value) throws SQLException {
		final int[] indexes = getIndexes(name);
		for (final int indexe : indexes) {
			this.statement.setLong(indexe, value);
		}
	}

	public void setTimestamp(final String name, final Timestamp value) throws SQLException
	{
		final int[] indexes = getIndexes(name);
		for (final int indexe : indexes) {
			this.statement.setTimestamp(indexe, value);
		}
	}

	public void setBoolean(final String name, final boolean value) throws SQLException {
		for (final int i : getIndexes(name)) {
			this.statement.setBoolean(i, value);
		}
	}

	public void setDate(final String name, final Date value) throws SQLException {
		for (final int index : getIndexes(name)) {
			this.statement.setDate(index, value);
		}
	}

	public void setDate(final String name, final java.util.Date value) throws SQLException {
		this.setDate(name, new java.sql.Date(value.getTime()));
	}

	public void setArray(final String name, final Array value) throws SQLException {
		for (final int index : getIndexes(name)) {
			this.statement.setArray(index, value);
		}
	}

	public PreparedStatement getStatement() {
		return this.statement;
	}

	public boolean execute() throws SQLException {
		return this.statement.execute();
	}

	public ResultSet executeQuery() throws SQLException {
		return this.statement.executeQuery();
	}

	public int executeUpdate() throws SQLException {
		return this.statement.executeUpdate();
	}

	public void close() throws SQLException {
		this.statement.close();
	}

	public void addBatch() throws SQLException {
		this.statement.addBatch();
	}

	public int[] executeBatch() throws SQLException {
		return this.statement.executeBatch();
	}
}