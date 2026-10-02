package io.github.mpiumakkho.branchsales.producer.synclog;

import java.time.Clock;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.UUID;

import org.jspecify.annotations.Nullable;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Writes the result of one send attempt for a (sale_date, revision):
 * <ul>
 * <li>{@code sync_log}: current status, one row per revision, updated on every attempt</li>
 * <li>{@code sync_attempt}: history, one row per attempt, never updated</li>
 * </ul>
 * Both are written in one transaction, so the history always matches the status.
 * <p>
 * Update-then-insert instead of a vendor upsert statement, so the same SQL runs on PostgreSQL, MySQL and SQL Server
 * (step 7). One producer runs per branch, so two writers for the same row are not expected.
 */
@Repository
public class SyncLog {

	static final int MAX_ERROR_LENGTH = 1000;

	private final JdbcClient jdbc;
	private final TransactionTemplate transaction;
	private final Clock clock;

	public SyncLog(JdbcClient jdbc, TransactionTemplate transaction, Clock clock) {
		this.jdbc = jdbc;
		this.transaction = transaction;
		this.clock = clock;
	}

	/** Called only after the broker acknowledged the message. */
	public void markSent(LocalDate saleDate, int revision, UUID eventId) {
		OffsetDateTime now = OffsetDateTime.now(clock);
		transaction.executeWithoutResult(status -> {
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
			recordAttempt(saleDate, revision, now, "SENT", null, eventId);
		});
	}

	/**
	 * The revision stays pending and is tried again in the next round.
	 * @param eventId the message that was not acknowledged, or null if no message could be written from the data.
	 *                A message that was not acknowledged may still have reached Kafka, so its eventId can show up at HQ.
	 */
	public void markFailed(LocalDate saleDate, int revision, String error, @Nullable UUID eventId) {
		OffsetDateTime now = OffsetDateTime.now(clock);
		String lastError = error.length() > MAX_ERROR_LENGTH ? error.substring(0, MAX_ERROR_LENGTH) : error;
		transaction.executeWithoutResult(status -> {
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
			recordAttempt(saleDate, revision, now, "FAILED", lastError, eventId);
		});
	}

	private void recordAttempt(LocalDate saleDate, int revision, OffsetDateTime attemptedAt, String result,
			@Nullable String error, @Nullable UUID eventId) {
		jdbc.sql("""
				insert into sync_attempt (sale_date, revision, attempted_at, result, error, event_id)
				values (:saleDate, :revision, :attemptedAt, :result, :error, :eventId)
				""")
				.param("saleDate", saleDate)
				.param("revision", revision)
				.param("attemptedAt", attemptedAt)
				.param("result", result)
				.param("error", error)
				.param("eventId", eventId == null ? null : eventId.toString())
				.update();
	}
}
