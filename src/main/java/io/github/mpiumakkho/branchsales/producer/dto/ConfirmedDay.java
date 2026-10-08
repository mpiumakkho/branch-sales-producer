package io.github.mpiumakkho.branchsales.producer.dto;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.List;

import org.jspecify.annotations.Nullable;

/**
 * One record of one record type as stored in the back-office, ready to send (a confirmed day, a closed shift), with
 * its local line codes.
 *
 * @param key   the business day, or (business day, terminal, shift) for a shift close
 * @param shift the shift close header fields; null for the daily types
 */
public record ConfirmedDay(
		RecordType type,
		String branchCode,
		RecordKey key,
		int revision,
		OffsetDateTime confirmedAt,
		@Nullable ShiftDetail shift,
		List<Line> lines) {

	public ConfirmedDay {
		lines = List.copyOf(lines);
	}

	/** The business date of the record. */
	public LocalDate date() {
		return key.date();
	}

	/** @param localCode the back-office's own category or tender code */
	public record Line(String localCode, BigDecimal amount, long quantity) {
	}

	/**
	 * Header fields of a shift close. Nullable where the back-office columns are; the message writer rejects nulls.
	 */
	public record ShiftDetail(
			@Nullable String cashierId,
			OffsetDateTime openedAt,
			@Nullable OffsetDateTime closedAt,
			long transactionCount,
			@Nullable BigDecimal cashExpected,
			@Nullable BigDecimal cashCounted) {
	}
}
