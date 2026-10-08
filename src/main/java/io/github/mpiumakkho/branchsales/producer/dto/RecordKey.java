package io.github.mpiumakkho.branchsales.producer.dto;

import java.time.LocalDate;

/**
 * What identifies one record of a type within the branch, apart from its revision: a business day for the daily
 * types, (business day, terminal, shift) for a shift close. Part of the {@code sync_state} id ({@link SyncState#id}).
 */
public sealed interface RecordKey permits RecordKey.DayKey, RecordKey.ShiftKey {

	/** The business date of the record. */
	LocalDate date();

	/** The key's part of the {@code sync_state} id, between the type and the revision. Never parsed. */
	String idPart();

	/** For log lines and error messages. */
	String text();

	/** One business day ({@code sale_date}, {@code return_date}). */
	record DayKey(LocalDate date) implements RecordKey {

		@Override
		public String idPart() {
			return date.toString();
		}

		@Override
		public String text() {
			return date.toString();
		}
	}

	/**
	 * One shift of one POS terminal.
	 *
	 * @param date       the business date the shift was opened under
	 * @param terminalId never contains {@code #} (contract pattern, checked before a message is written)
	 */
	record ShiftKey(LocalDate date, String terminalId, int shiftNo) implements RecordKey {

		@Override
		public String idPart() {
			return date + "#" + terminalId + "#" + shiftNo;
		}

		@Override
		public String text() {
			return date + " " + terminalId + " shift " + shiftNo;
		}
	}
}
