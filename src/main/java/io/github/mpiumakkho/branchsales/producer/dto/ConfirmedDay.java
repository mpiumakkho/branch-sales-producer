package io.github.mpiumakkho.branchsales.producer.dto;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.List;

/**
 * One confirmed business day of one record type as stored in the back-office, with its local category codes.
 *
 * @param date the business date: {@code sale_date} of the sales, {@code return_date} of the returns
 */
public record ConfirmedDay(
		RecordType type,
		String branchCode,
		LocalDate date,
		int revision,
		OffsetDateTime confirmedAt,
		List<Line> lines) {

	public ConfirmedDay {
		lines = List.copyOf(lines);
	}

	public record Line(String localCategoryCode, BigDecimal amount, long quantity) {
	}
}
