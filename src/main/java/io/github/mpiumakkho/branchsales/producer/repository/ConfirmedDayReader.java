package io.github.mpiumakkho.branchsales.producer.repository;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.jspecify.annotations.Nullable;
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
import io.github.mpiumakkho.branchsales.producer.dto.RecordKey;
import io.github.mpiumakkho.branchsales.producer.dto.RecordKey.DayKey;
import io.github.mpiumakkho.branchsales.producer.dto.RecordKey.ShiftKey;
import io.github.mpiumakkho.branchsales.producer.dto.RecordType;

/**
 * Reads the records of one record type that are ready to send (rule R1: confirmed days, closed shifts) with their
 * lines, from a given date on: daily sales from {@code daily_sales}, daily returns from {@code daily_return}, shift
 * closes from {@code pos_shift}. Which of them still need sending is decided by their send state in MongoDB
 * ({@code SendRound}).
 * Read-only: the back-office tables belong to the back-office system, and the producer writes nothing there.
 * <p>
 * SQL is kept to what PostgreSQL, MySQL and SQL Server all accept. Table and column names come from the
 * {@link RecordType} enum, not from input.
 * <p>
 * Headers and lines are read in one transaction that sees a single snapshot, so the lines always belong to the
 * revision read, even if the manager edits the day meanwhile, and the back-office's writes are never blocked:
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
			List<String> keyColumns = keyColumns(type);
			String keys = String.join(", ", keyColumns);
			// The lines' key is read from the header, not the line: on MySQL and SQL Server the join compares with the
			// column collation (case-insensitive by default, SQL Server also ignores trailing spaces), so a line can join
			// a header whose key text differs from its own. Grouping by the header's values keeps such lines with it.
			String lineKeys = keyColumns.stream().map(c -> "d." + c).collect(Collectors.joining(", "));
			String join = keyColumns.stream().map(c -> "d." + c + " = l." + c).collect(Collectors.joining(" and "));
			String confirmedAt = type.confirmedAtColumn().equals("confirmed_at") ? "confirmed_at"
					: type.confirmedAtColumn() + " as confirmed_at";
			String shiftColumns = switch (type.keyKind()) {
				case DAY -> "";
				case SHIFT -> ", cashier_id, opened_at, closed_at, transaction_count, cash_expected, cash_counted";
			};
			String code = type.lineKind().localCodeColumn();
			return new Sql(
					"""
					select %1$s, branch_code, revision, %2$s%3$s
					  from %4$s
					 where status = '%5$s'
					   and %6$s >= :since
					 order by %1$s
					""".formatted(keys, confirmedAt, shiftColumns, type.table(), type.readyStatus(), type.dateColumn()),
					"""
					select %1$s, l.%2$s, l.amount, l.quantity
					  from %3$s l
					  join %4$s d on %5$s
					 where d.status = '%6$s'
					   and d.%7$s >= :since
					 order by %1$s, l.%2$s
					""".formatted(lineKeys, code, type.lineTable(), type.table(), join, type.readyStatus(),
							type.dateColumn()));
		}

		/** The key columns of both tables, in key order. */
		static List<String> keyColumns(RecordType type) {
			return switch (type.keyKind()) {
				case DAY -> List.of(type.dateColumn());
				case SHIFT -> List.of(type.dateColumn(), "terminal_id", "shift_no");
			};
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

	/**
	 * Records of the type ready to send, from {@code since} on, oldest first (in key order), so HQ receives them in
	 * order.
	 */
	public List<ConfirmedDay> readConfirmed(RecordType type, LocalDate since) {
		Sql statements = sql.get(type);
		String code = type.lineKind().localCodeColumn();
		return snapshot.execute(status -> {
			if (sqlServer) {
				// Spring accepts only the standard levels, so set SNAPSHOT on the transaction's connection before its
				// first statement. The connection pool restores the default level when the connection is returned.
				useSqlServerSnapshot();
			}
			// Grouped by the whole key: two shifts of one day have their own lines
			Map<RecordKey, List<ConfirmedDay.Line>> linesByKey = new HashMap<>();
			jdbc.sql(statements.lines()).param("since", since).query(rs -> {
				linesByKey.computeIfAbsent(key(type, rs), k -> new ArrayList<>())
						.add(new ConfirmedDay.Line(rs.getString(code), rs.getBigDecimal("amount"), rs.getLong("quantity")));
			});
			return jdbc.sql(statements.days())
					.param("since", since)
					.query((rs, n) -> {
						RecordKey key = key(type, rs);
						return new ConfirmedDay(
								type,
								rs.getString("branch_code"),
								key,
								rs.getInt("revision"),
								rs.getObject("confirmed_at", OffsetDateTime.class),
								shiftDetail(type, rs),
								linesByKey.getOrDefault(key, List.of()));
					})
					.list();
		});
	}

	private static RecordKey key(RecordType type, ResultSet rs) throws SQLException {
		LocalDate date = rs.getObject(type.dateColumn(), LocalDate.class);
		return switch (type.keyKind()) {
			case DAY -> new DayKey(date);
			case SHIFT -> new ShiftKey(date, rs.getString("terminal_id"), rs.getInt("shift_no"));
		};
	}

	private static ConfirmedDay.@Nullable ShiftDetail shiftDetail(RecordType type, ResultSet rs) throws SQLException {
		return switch (type.keyKind()) {
			case DAY -> null;
			case SHIFT -> new ConfirmedDay.ShiftDetail(
					rs.getString("cashier_id"),
					rs.getObject("opened_at", OffsetDateTime.class),
					rs.getObject("closed_at", OffsetDateTime.class),
					rs.getLong("transaction_count"),
					rs.getBigDecimal("cash_expected"),
					rs.getBigDecimal("cash_counted"));
		};
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
