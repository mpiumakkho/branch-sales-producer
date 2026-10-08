package io.github.mpiumakkho.branchsales.producer.dto;

import org.jspecify.annotations.Nullable;

/**
 * The fields of a {@code DailySalesReceipt} (contract/daily-sales-receipt.v1.schema.json) the producer uses.
 *
 * @param type         which record the receipt is for; receipts are matched by (type, sourceOffset) since offsets
 *                     are per topic. A receipt without {@code type} is for a DAILY_SUMMARY
 * @param sourceOffset offset of the record this receipt is for, in the topic of its type
 * @param outcome      INSERTED, UPDATED, DUPLICATE, STALE or REJECTED
 */
public record HqReceipt(
		RecordType type,
		String branchCode,
		long sourceOffset,
		String outcome,
		@Nullable Integer storedRevision,
		@Nullable String rejectReason,
		@Nullable String detail) {

	/** HQ holds this revision or a higher one. */
	public boolean accepted() {
		return !"REJECTED".equals(outcome);
	}
}
