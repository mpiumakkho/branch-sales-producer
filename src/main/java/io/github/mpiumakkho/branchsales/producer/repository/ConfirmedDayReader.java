package io.github.mpiumakkho.branchsales.producer.repository;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.jdbc.support.JdbcUtils;
import org.springframework.jdbc.support.MetaDataAccessException;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import io.github.mpiumakkho.branchsales.producer.dto.ConfirmedDay;
import io.github.mpiumakkho.branchsales.producer.dto.RecordType;

/**
 * Reads confirmed days (rule R1) of one record type with their lines, from a given date on: daily sales from
 * {@code daily_sales}, daily returns from {@code daily_return}, same shape. Which of them still need sending is
 * decided by their send state in MongoDB ({@code SendRound}).
 * Read-only: the back-office tables belong to the back-office system, and the producer writes nothing there.
 * <p>
 * SQL is kept to what PostgreSQL, MySQL and SQL Server all accept. Table and column names come from the
 * {@link RecordType} enum, not from input.
 * <p>
 * Days and lines are read in one transaction that sees a single snapshot, so the lines always belong to the revision
 * read, even if the manager edits the day meanwhile, and the back-office's writes are never blocked:
 * <ul>
 * <li>PostgreSQL, MySQL (InnoDB): REPEATABLE READ, which reads from a snapshot</li>
 * <li>SQL Server: SNAPSHOT. Its REPEATABLE READ is lock-based instead: it would block back-office writes and could
 * deadlock with them. Requires {@code ALLOW_SNAPSHOT_ISOLATION ON} for the database (backoffice/sqlserver/schema.sql)</li>
 * </ul>
 */
@Component
public class ConfirmedDayReader {

	private record Sql(String days, String lines) {

		static Sql of(RecordType type) {
			String d = type.dateColumn();
			return new Sql(
					"""
					select %1$s, branch_code, revision, confirmed_at
					  from %2$s
					 where status = 'CONFIRMED'
					   and %1$s >= :since
					 order by %1$s
					""".formatted(d, type.table()),
					"""
					select l.%1$s, l.category_code, l.amount, l.quantity
					  from %3$s l
					  join %2$s d on d.%1$s = l.%1$s
					 where d.status = 'CONFIRMED'
					   and d.%1$s >= :since
					 order by l.%1$s, l.category_code
					""".formatted(d, type.table(), type.lineTable()));
		}
	}

	// SQLServerConnection.TRANSACTION_SNAPSHOT; not in java.sql.Connection, and the driver is only a runtime dependency
	private static final int SQL_SERVER_SNAPSHOT = 4096;

	private final JdbcClient jdbc;
	private final DataSource dataSource;
	private final TransactionTemplate snapshot;
	private final boolean sqlServer;
	private final Map<RecordType, Sql> sql = new EnumMap<>(RecordType.class);

	public ConfirmedDayReader(JdbcClient jdbc, DataSource dataSource, PlatformTransactionManager transactionManager) {
		this.jdbc = jdbc;
		this.dataSource = dataSource;
		this.sqlServer = "Microsoft SQL Server".equals(databaseProductName(dataSource));
		this.snapshot = new TransactionTemplate(transactionManager);
		this.snapshot.setReadOnly(true);
		if (!sqlServer) {
			this.snapshot.setIsolationLevel(TransactionDefinition.ISOLATION_REPEATABLE_READ);
		}
		for (RecordType type : RecordType.values()) {
			sql.put(type, Sql.of(type));
		}
	}

	/** Confirmed days of the type from {@code since} on, oldest first, so HQ receives days in order. */
	public List<ConfirmedDay> readConfirmed(RecordType type, LocalDate since) {
		Sql statements = sql.get(type);
		String dateColumn = type.dateColumn();
		return snapshot.execute(status -> {
			if (sqlServer) {
				// Spring accepts only the standard levels, so set SNAPSHOT on the transaction's connection before its
				// first statement. The connection pool restores the default level when the connection is returned.
				useSqlServerSnapshot();
			}
			Map<LocalDate, List<ConfirmedDay.Line>> linesByDay = new HashMap<>();
			jdbc.sql(statements.lines()).param("since", since).query(rs -> {
				linesByDay.computeIfAbsent(rs.getObject(dateColumn, LocalDate.class), d -> new ArrayList<>())
						.add(new ConfirmedDay.Line(rs.getString("category_code"), rs.getBigDecimal("amount"),
								rs.getLong("quantity")));
			});
			return jdbc.sql(statements.days())
					.param("since", since)
					.query((rs, n) -> {
						LocalDate date = rs.getObject(dateColumn, LocalDate.class);
						return new ConfirmedDay(
								type,
								rs.getString("branch_code"),
								date,
								rs.getInt("revision"),
								rs.getObject("confirmed_at", OffsetDateTime.class),
								linesByDay.getOrDefault(date, List.of()));
					})
					.list();
		});
	}

	private void useSqlServerSnapshot() {
		try {
			DataSourceUtils.getConnection(dataSource).setTransactionIsolation(SQL_SERVER_SNAPSHOT);
		}
		catch (SQLException e) {
			throw new CannotGetJdbcConnectionException("cannot set SNAPSHOT isolation", e);
		}
	}

	private static String databaseProductName(DataSource dataSource) {
		try {
			return JdbcUtils.extractDatabaseMetaData(dataSource, DatabaseMetaData::getDatabaseProductName);
		}
		catch (MetaDataAccessException e) {
			throw new IllegalStateException("cannot read the branch database product name", e);
		}
	}
}
