package io.github.mpiumakkho.branchsales.producer.backoffice;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Reads confirmed days whose current revision has not been sent yet (rule R1).
 * Read-only: the back-office tables belong to the back-office system.
 * <p>
 * SQL is kept to what PostgreSQL, MySQL and SQL Server all accept (step 7).
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

	private final JdbcClient jdbc;
	private final TransactionTemplate snapshot;

	public PendingSalesReader(JdbcClient jdbc, PlatformTransactionManager transactionManager) {
		this.jdbc = jdbc;
		// Days and lines are read in one repeatable-read transaction, so the lines belong to the revision read,
		// even if the manager edits the day while the producer is reading.
		this.snapshot = new TransactionTemplate(transactionManager);
		this.snapshot.setIsolationLevel(TransactionDefinition.ISOLATION_REPEATABLE_READ);
		this.snapshot.setReadOnly(true);
	}

	/** Oldest day first, so HQ receives days in order. */
	public List<ConfirmedSales> readPending() {
		return snapshot.execute(status -> {
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
}
