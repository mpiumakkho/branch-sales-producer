package io.github.mpiumakkho.branchsales.producer.synclog;

import java.time.Clock;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.UUID;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/**
 * Writes the send status of one (sale_date, revision) to {@code sync_log}.
 * <p>
 * Update-then-insert instead of a vendor upsert statement, so the same SQL runs on PostgreSQL, MySQL and SQL Server
 * (step 7). One producer runs per branch, so two writers for the same row are not expected.
 */
@Repository
public class SyncLog {

	static final int MAX_ERROR_LENGTH = 1000;

	private final JdbcClient jdbc;
	private final Clock clock;

	public SyncLog(JdbcClient jdbc, Clock clock) {
		this.jdbc = jdbc;
		this.clock = clock;
	}

	/** Called only after the broker acknowledged the message. */
	public void markSent(LocalDate saleDate, int revision, UUID eventId) {
		OffsetDateTime now = OffsetDateTime.now(clock);
		int updated = jdbc.sql("""
				update sync_log
				   set status = 'SENT', attempts = attempts + 1, last_error = null, event_id = :eventId,
				       sent_at = :now, updated_at = :now
				 where sale_date = :saleDate and revision = :revision
				""")
				.param("eventId", eventId.toString())
				.param("now", now)
				.param("saleDate", saleDate)
				.param("revision", revision)
				.update();
		if (updated == 0) {
			jdbc.sql("""
					insert into sync_log (sale_date, revision, status, attempts, last_error, event_id, sent_at, updated_at)
					values (:saleDate, :revision, 'SENT', 1, null, :eventId, :now, :now)
					""")
					.param("saleDate", saleDate)
					.param("revision", revision)
					.param("eventId", eventId.toString())
					.param("now", now)
					.update();
		}
	}

	/** The revision stays pending and is tried again in the next round. */
	public void markFailed(LocalDate saleDate, int revision, String error) {
		OffsetDateTime now = OffsetDateTime.now(clock);
		String lastError = error.length() > MAX_ERROR_LENGTH ? error.substring(0, MAX_ERROR_LENGTH) : error;
		int updated = jdbc.sql("""
				update sync_log
				   set status = 'FAILED', attempts = attempts + 1, last_error = :error, updated_at = :now
				 where sale_date = :saleDate and revision = :revision
				""")
				.param("error", lastError)
				.param("now", now)
				.param("saleDate", saleDate)
				.param("revision", revision)
				.update();
		if (updated == 0) {
			jdbc.sql("""
					insert into sync_log (sale_date, revision, status, attempts, last_error, updated_at)
					values (:saleDate, :revision, 'FAILED', 1, :error, :now)
					""")
					.param("saleDate", saleDate)
					.param("revision", revision)
					.param("error", lastError)
					.param("now", now)
					.update();
		}
	}
}
