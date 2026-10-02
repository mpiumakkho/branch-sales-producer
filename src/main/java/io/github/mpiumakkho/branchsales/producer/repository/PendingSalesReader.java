package io.github.mpiumakkho.branchsales.producer.repository;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.ArrayList;
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

import io.github.mpiumakkho.branchsales.producer.dto.ConfirmedSales;

/**
 * Reads confirmed days whose current revision has not been sent yet (rule R1).
 * Read-only: the back-office tables belong to the back-office system.
 * <p>
 * SQL is kept to what PostgreSQL, MySQL and SQL Server all accept.
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
public class PendingSalesReader {

	private static final String PENDING_DAYS = """
			select d.sale_date, d.branch_code, d.revision, d.confirmed_at
			  from daily_sales d
			  left join sync_log s
			    on s.sale_date = d.sale_date and s.revision = d.revision and s.status = 'SENT'
			 where d.status = 'CONFIRMED'
			   and s.sale_date is null
			 order by d.sale_date
			""";

	private static final String LINES_OF_PENDING_DAYS = """
			select l.sale_date, l.category_code, l.amount, l.quantity
			  from daily_sales_line l
			  join daily_sales d on d.sale_date = l.sale_date
			  left join sync_log s
			    on s.sale_date = d.sale_date and s.revision = d.revision and s.status = 'SENT'
			 where d.status = 'CONFIRMED'
			   and s.sale_date is null
			 order by l.sale_date, l.category_code
			""";

	// SQLServerConnection.TRANSACTION_SNAPSHOT; not in java.sql.Connection, and the driver is only a runtime dependency
	private static final int SQL_SERVER_SNAPSHOT = 4096;

	private final JdbcClient jdbc;
	private final DataSource dataSource;
	private final TransactionTemplate snapshot;
	private final boolean sqlServer;

	public PendingSalesReader(JdbcClient jdbc, DataSource dataSource, PlatformTransactionManager transactionManager) {
		this.jdbc = jdbc;
		this.dataSource = dataSource;
		this.sqlServer = "Microsoft SQL Server".equals(databaseProductName(dataSource));
		this.snapshot = new TransactionTemplate(transactionManager);
		this.snapshot.setReadOnly(true);
		if (!sqlServer) {
			this.snapshot.setIsolationLevel(TransactionDefinition.ISOLATION_REPEATABLE_READ);
		}
	}

	/** Oldest day first, so HQ receives days in order. */
	public List<ConfirmedSales> readPending() {
		return snapshot.execute(status -> {
			if (sqlServer) {
				// Spring accepts only the standard levels, so set SNAPSHOT on the transaction's connection before its
				// first statement. The connection pool restores the default level when the connection is returned.
				useSqlServerSnapshot();
			}
			Map<LocalDate, List<ConfirmedSales.Line>> linesByDay = new HashMap<>();
			jdbc.sql(LINES_OF_PENDING_DAYS).query(rs -> {
				linesByDay.computeIfAbsent(rs.getObject("sale_date", LocalDate.class), d -> new ArrayList<>())
						.add(new ConfirmedSales.Line(rs.getString("category_code"), rs.getBigDecimal("amount"),
								rs.getLong("quantity")));
			});
			return jdbc.sql(PENDING_DAYS)
					.query((rs, n) -> {
						LocalDate saleDate = rs.getObject("sale_date", LocalDate.class);
						return new ConfirmedSales(
								rs.getString("branch_code"),
								saleDate,
								rs.getInt("revision"),
								rs.getObject("confirmed_at", OffsetDateTime.class),
								linesByDay.getOrDefault(saleDate, List.of()));
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
