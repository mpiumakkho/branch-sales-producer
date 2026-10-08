package io.github.mpiumakkho.branchsales.producer.dto;

import java.time.Instant;
import java.time.LocalDate;

import org.jspecify.annotations.Nullable;

/**
 * Send state of one (type, date, revision), one MongoDB document in {@code sync_state} (requirements §6).
 *
 * @param sentAt when the last message for this revision was acknowledged by the branch broker, if ever
 */
public record SyncState(RecordType type, LocalDate date, int revision, Status status, int attempts,
		@Nullable Instant sentAt) {

	public enum Status {
		/** The branch broker acknowledged a message; no HQ receipt yet. */
		SENT,
		/** No message could be written, or the branch broker did not acknowledge it. Sent again next round. */
		FAILED,
		/** HQ holds this revision or a higher one (receipt INSERTED, UPDATED, DUPLICATE or STALE). Final. */
		HQ_ACCEPTED,
		/** HQ rejected the message and keeps it in its dead_letter table. Not sent again by the producer. */
		HQ_REJECTED
	}

	/** Document id: one per (type, date, revision), e.g. {@code DAILY_SUMMARY#2026-10-01#1}. */
	public static String id(RecordType type, LocalDate date, int revision) {
		return type + "#" + date + "#" + revision;
	}
}
