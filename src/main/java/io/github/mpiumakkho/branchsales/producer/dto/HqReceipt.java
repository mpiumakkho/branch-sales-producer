package io.github.mpiumakkho.branchsales.producer.dto;

import org.jspecify.annotations.Nullable;

/**
 * The fields of a {@code DailySalesReceipt} (contract/daily-sales-receipt.v1.schema.json) the producer uses.
 *
 * @param sourceOffset offset of the summary record this receipt is for
 * @param outcome      INSERTED, UPDATED, DUPLICATE, STALE or REJECTED
 */
public record HqReceipt(
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
