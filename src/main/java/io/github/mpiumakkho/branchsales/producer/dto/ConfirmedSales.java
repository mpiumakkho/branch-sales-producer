package io.github.mpiumakkho.branchsales.producer.dto;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.List;

/**
 * One confirmed day as stored in the back-office, with its local category codes.
 */
public record ConfirmedSales(
		String branchCode,
		LocalDate saleDate,
		int revision,
		OffsetDateTime confirmedAt,
		List<Line> lines) {

	public ConfirmedSales {
		lines = List.copyOf(lines);
	}

	public record Line(String localCategoryCode, BigDecimal amount, long quantity) {
	}
}
