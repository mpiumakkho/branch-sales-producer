package io.github.mpiumakkho.branchsales.producer.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import io.github.mpiumakkho.branchsales.producer.ContractSchema;
import io.github.mpiumakkho.branchsales.producer.config.ProducerProperties;
import io.github.mpiumakkho.branchsales.producer.dto.ConfirmedDay;
import io.github.mpiumakkho.branchsales.producer.dto.ConfirmedDay.Line;
import io.github.mpiumakkho.branchsales.producer.dto.ConfirmedDay.ShiftDetail;
import io.github.mpiumakkho.branchsales.producer.dto.RecordKey.DayKey;
import io.github.mpiumakkho.branchsales.producer.dto.RecordKey.ShiftKey;
import io.github.mpiumakkho.branchsales.producer.dto.RecordType;
import io.github.mpiumakkho.branchsales.producer.exception.InvalidSalesException;

class DailyMessageWriterTest {

	private static final Map<String, String> MAPPING = Map.of(
			"BEV", "BEVERAGE",
			"DRINK_HOT", "BEVERAGE",
			"SNK", "SNACK",
			"RTE", "READY_MEAL");

	private static final Map<String, String> TENDERS = Map.of(
			"CSH", "CASH",
			"CSH2", "CASH",
			"CRD", "CREDIT_CARD",
			"DBT", "DEBIT_CARD",
			"QR", "QR_PAYMENT");

	private final DailyMessageWriter writer = writer(TENDERS);

	private static final DayKey DAY = new DayKey(LocalDate.of(2026, 10, 1));

	private final JsonMapper mapper = JsonMapper.builder().build();

	@Test
	void writesValidContractMessage() {
		var message = writer.write(sales(
				new Line("BEV", new BigDecimal("18200.00"), 410),
				new Line("SNK", new BigDecimal("12050"), 395),
				new Line("RTE", new BigDecimal("9120.5"), 152)));

		assertThat(ContractSchema.errors(message.value())).isEmpty();
		assertThat(message.key()).isEqualTo("BR0001");

		JsonNode json = mapper.readTree(message.value());
		assertThat(json.get("schemaVersion").intValue()).isEqualTo(1);
		assertThat(json.get("eventId").asString()).isEqualTo(message.eventId().toString());
		assertThat(json.get("branchCode").asString()).isEqualTo("BR0001");
		assertThat(json.get("saleDate").asString()).isEqualTo("2026-10-01");
		assertThat(json.get("revision").intValue()).isEqualTo(2);
		assertThat(json.get("currency").asString()).isEqualTo("THB");
		// Money as strings with exactly two decimals; total = sum of lines (R8)
		assertThat(json.get("totalAmount").asString()).isEqualTo("39370.50");
		assertThat(json.get("lines").toString()).isEqualTo("""
				[{"categoryCode":"BEVERAGE","amount":"18200.00","quantity":410},\
				{"categoryCode":"READY_MEAL","amount":"9120.50","quantity":152},\
				{"categoryCode":"SNACK","amount":"12050.00","quantity":395}]""");
	}

	@Test
	void writesConfirmedAtInBangkokTimeWithSeconds() {
		// 14:45:00 UTC = 21:45:00 in Bangkok; seconds must be present for RFC 3339
		var message = writer.write(new ConfirmedDay(RecordType.DAILY_SUMMARY, "BR0001", DAY, 1,
				OffsetDateTime.parse("2026-10-01T14:45:00Z"), null, List.of(new Line("BEV", BigDecimal.TEN, 1))));

		assertThat(mapper.readTree(message.value()).get("confirmedAt").asString()).isEqualTo("2026-10-01T21:45:00+07:00");
		assertThat(ContractSchema.errors(message.value())).isEmpty();
	}

	@Test
	void schemaCheckRejectsDateTimeWithoutSeconds() {
		// What OffsetDateTime.toString() would produce; shows the schema check in these tests really checks formats
		var message = writer.write(sales(new Line("BEV", BigDecimal.ONE, 1)));
		String json = new String(message.value(), StandardCharsets.UTF_8)
				.replace("2026-10-01T21:45:00+07:00", "2026-10-01T21:45+07:00");

		assertThat(ContractSchema.errors(json.getBytes(StandardCharsets.UTF_8)))
				.singleElement().asString().contains("/confirmedAt");
	}

	@Test
	void addsUpLocalCategoriesMappedToTheSameHqCategory() {
		var message = writer.write(sales(
				new Line("BEV", new BigDecimal("100.00"), 2),
				new Line("DRINK_HOT", new BigDecimal("50.25"), 3)));

		JsonNode json = mapper.readTree(message.value());
		assertThat(json.get("lines").toString())
				.isEqualTo("[{\"categoryCode\":\"BEVERAGE\",\"amount\":\"150.25\",\"quantity\":5}]");
		assertThat(json.get("totalAmount").asString()).isEqualTo("150.25");
		assertThat(ContractSchema.errors(message.value())).isEmpty();
	}

	@Test
	void eachMessageHasItsOwnEventId() {
		var day = sales(new Line("BEV", BigDecimal.ONE, 1));
		assertThat(writer.write(day).eventId()).isNotEqualTo(writer.write(day).eventId());
	}

	@Test
	void rejectsUnmappedCategory() {
		assertThatThrownBy(() -> writer.write(sales(
				new Line("BEV", BigDecimal.ONE, 1),
				new Line("LOTTO", BigDecimal.ONE, 1),
				new Line("CIG", BigDecimal.ONE, 1))))
				.isInstanceOf(InvalidSalesException.class)
				.hasMessage("no HQ category mapping for local category [CIG, LOTTO]");
	}

	@Test
	void rejectsDataTheContractDoesNotAllow() {
		assertInvalid(sales(), "confirmed day has no lines");
		assertInvalid(sales(new Line("BEV", new BigDecimal("-1.00"), 1)), "negative amount for category BEV");
		assertInvalid(sales(new Line("BEV", BigDecimal.ONE, -1)), "negative quantity for category BEV");
		assertInvalid(sales(new Line("BEV", new BigDecimal("1.005"), 1)), "amount with more than 2 decimals for category BEV");
		assertInvalid(new ConfirmedDay(RecordType.DAILY_SUMMARY, "BR0001", DAY, 1, null, null,
				List.of(new Line("BEV", BigDecimal.ONE, 1))), "confirmed day has no confirmed_at");
	}

	@Test
	void rejectsDayOfAnotherBranch() {
		// e.g. the back-office database was copied from another branch
		assertInvalid(new ConfirmedDay(RecordType.DAILY_SUMMARY, "BR0009", DAY, 1,
				OffsetDateTime.parse("2026-10-01T21:45:00+07:00"), null, List.of(new Line("BEV", BigDecimal.ONE, 1))),
				"daily_sales.branch_code 'BR0009' does not match configured branch code BR0001");
	}

	private void assertInvalid(ConfirmedDay sales, String message) {
		assertThatThrownBy(() -> writer.write(sales)).isInstanceOf(InvalidSalesException.class).hasMessage(message);
	}

	@Test
	void writesReturnMessageWithReturnDate() {
		var message = writer.write(new ConfirmedDay(RecordType.DAILY_RETURN, "BR0001", DAY, 1,
				OffsetDateTime.parse("2026-10-01T21:50:00+07:00"), null, List.of(new Line("BEV", new BigDecimal("120.00"), 3))));

		assertThat(ContractSchema.returnErrors(message.value())).isEmpty();
		assertThat(ContractSchema.errors(message.value())).isNotEmpty(); // not a valid summary: no saleDate
		JsonNode json = mapper.readTree(message.value());
		assertThat(json.get("returnDate").asString()).isEqualTo("2026-10-01");
		assertThat(json.has("saleDate")).isFalse();
		assertThat(json.get("totalAmount").asString()).isEqualTo("120.00");
	}

	private static ConfirmedDay sales(Line... lines) {
		return new ConfirmedDay(RecordType.DAILY_SUMMARY, "BR0001", DAY, 2,
				OffsetDateTime.parse("2026-10-01T21:45:00+07:00"), null, List.of(lines));
	}

	private static DailyMessageWriter writer(Map<String, String> tenderMapping) {
		return new DailyMessageWriter(new ProducerProperties("BR0001", "branch-sales.daily-summary",
				"branch-sales.daily-return", "branch-sales.shift-close", "branch-sales.receipt", MAPPING, tenderMapping,
				new ProducerProperties.Schedule("-", Duration.ZERO, "-"), Duration.ofSeconds(1), Duration.ofDays(60),
				Duration.ofHours(24), Duration.ofDays(90)));
	}

	// Shift close

	private static final ShiftKey SHIFT = new ShiftKey(LocalDate.of(2026, 10, 1), "POS01", 1);
	private static final OffsetDateTime CLOSED_AT = OffsetDateTime.parse("2026-10-01T15:02:11+07:00");
	// 00:00 UTC = 07:00 in Bangkok
	private static final OffsetDateTime OPENED_AT = OffsetDateTime.parse("2026-10-01T00:00:00Z");

	@Test
	void writesValidShiftCloseMessage() {
		var message = writer.write(shift(SHIFT, detail("C101", CLOSED_AT, "9120.00", "9100.00"),
				new Line("CSH", new BigDecimal("9120.00"), 140),
				new Line("CRD", new BigDecimal("6230"), 48),
				new Line("QR", new BigDecimal("3100.0"), 24)));

		assertThat(ContractSchema.shiftErrors(message.value())).isEmpty();
		assertThat(ContractSchema.errors(message.value())).isNotEmpty(); // not a valid summary: no saleDate
		assertThat(message.key()).isEqualTo("BR0001");
		// Field order is fixed: key fields, then the header, then money and lines
		assertThat(new String(message.value(), StandardCharsets.UTF_8)).isEqualTo("""
				{"schemaVersion":1,"eventId":"%s","branchCode":"BR0001","businessDate":"2026-10-01",\
				"terminalId":"POS01","shiftNo":1,"revision":1,"confirmedAt":"2026-10-01T15:02:11+07:00",\
				"cashierId":"C101","openedAt":"2026-10-01T07:00:00+07:00","closedAt":"2026-10-01T15:02:11+07:00",\
				"transactionCount":212,"currency":"THB","totalAmount":"18450.00","cashExpected":"9120.00",\
				"cashCounted":"9100.00","lines":[{"tenderType":"CASH","amount":"9120.00","quantity":140},\
				{"tenderType":"CREDIT_CARD","amount":"6230.00","quantity":48},\
				{"tenderType":"QR_PAYMENT","amount":"3100.00","quantity":24}]}""".formatted(message.eventId()));
	}

	@Test
	void omitsCashierIdWhenTheBackOfficeHasNone() {
		var message = writer.write(shift(SHIFT, detail(null, CLOSED_AT, "0.00", "0.00"),
				new Line("CRD", new BigDecimal("100.00"), 1)));

		assertThat(ContractSchema.shiftErrors(message.value())).isEmpty();
		assertThat(mapper.readTree(message.value()).has("cashierId")).isFalse();
	}

	@Test
	void writesEmptyLinesForAShiftWithoutTransactions() {
		var message = writer.write(new ConfirmedDay(RecordType.SHIFT_CLOSE, "BR0001", SHIFT, 1, CLOSED_AT,
				new ShiftDetail("C101", OPENED_AT, CLOSED_AT, 0, new BigDecimal("2000.00"), new BigDecimal("2000.00")),
				List.of()));

		assertThat(ContractSchema.shiftErrors(message.value())).isEmpty();
		JsonNode json = mapper.readTree(message.value());
		assertThat(json.get("lines").toString()).isEqualTo("[]");
		assertThat(json.get("totalAmount").asString()).isEqualTo("0.00");
		assertThat(json.get("transactionCount").intValue()).isZero();
	}

	@Test
	void addsUpLocalTendersMappedToTheSameHqTender() {
		var message = writer.write(shift(SHIFT, detail("C101", CLOSED_AT, "150.25", "150.25"),
				new Line("CSH", new BigDecimal("100.00"), 2),
				new Line("CSH2", new BigDecimal("50.25"), 3)));

		JsonNode json = mapper.readTree(message.value());
		assertThat(json.get("lines").toString())
				.isEqualTo("[{\"tenderType\":\"CASH\",\"amount\":\"150.25\",\"quantity\":5}]");
		assertThat(json.get("totalAmount").asString()).isEqualTo("150.25");
		assertThat(ContractSchema.shiftErrors(message.value())).isEmpty();
	}

	@Test
	void rejectsUnmappedTender() {
		assertInvalid(shift(SHIFT, detail("C101", CLOSED_AT, "1.00", "1.00"),
				new Line("CSH", BigDecimal.ONE, 1),
				new Line("GV", BigDecimal.ONE, 1)),
				"no HQ tender mapping for local tender [GV]");
	}

	@Test
	void rejectsTerminalIdOutsideThePattern() {
		// # would also make the sync_state id ambiguous, where the terminal is one segment
		for (String terminal : List.of("pos01", "POS#01", "")) {
			assertInvalid(shift(new ShiftKey(SHIFT.date(), terminal, 1), detail("C101", CLOSED_AT, "1.00", "1.00"),
					new Line("CSH", BigDecimal.ONE, 1)),
					"pos_shift.terminal_id '" + terminal + "' does not match ^[A-Z0-9_-]{1,20}$");
		}
	}

	@Test
	void rejectsShiftClosedBeforeOpened() {
		OffsetDateTime closed = OffsetDateTime.parse("2026-10-01T06:59:59+07:00");
		assertInvalid(shift(SHIFT, detail("C101", closed, "1.00", "1.00"), new Line("CSH", BigDecimal.ONE, 1)),
				"closed_at 2026-10-01T06:59:59+07:00 is before opened_at 2026-10-01T00:00Z");
	}

	@Test
	void rejectsCashCountedWithMoreThanTwoDecimals() {
		assertInvalid(shift(SHIFT, detail("C101", CLOSED_AT, "1.00", "1.005"), new Line("CSH", BigDecimal.ONE, 1)),
				"cash_counted with more than 2 decimals: 1.005");
		assertInvalid(shift(SHIFT, detail("C101", CLOSED_AT, "-1.00", "1.00"), new Line("CSH", BigDecimal.ONE, 1)),
				"negative cash_expected -1.00");
	}

	@Test
	void rejectsMoreThanTwentyTenders() {
		// 21 local tenders, each mapped to its own HQ tender type (TENDER_A ... TENDER_U)
		Map<String, String> mapping = IntStream.range(0, 21).boxed()
				.collect(Collectors.toMap(i -> "T" + i, i -> "TENDER_" + (char) ('A' + i)));
		Line[] lines = IntStream.range(0, 21).mapToObj(i -> new Line("T" + i, BigDecimal.ONE, 1)).toArray(Line[]::new);

		assertThatThrownBy(() -> writer(mapping).write(shift(SHIFT, detail("C101", CLOSED_AT, "1.00", "1.00"), lines)))
				.isInstanceOf(InvalidSalesException.class)
				.hasMessage("21 tenders, the contract allows 20");
	}

	@Test
	void rejectsShiftWithoutClosedAtOrCashFigures() {
		Line cash = new Line("CSH", BigDecimal.ONE, 1);
		assertInvalid(shift(SHIFT, new ShiftDetail("C101", OPENED_AT, null, 1, BigDecimal.ONE, BigDecimal.ONE), cash),
				"closed shift has no closed_at");
		assertInvalid(shift(SHIFT, new ShiftDetail("C101", OPENED_AT, CLOSED_AT, 1, null, BigDecimal.ONE), cash),
				"closed shift has no cash_expected");
		assertInvalid(shift(SHIFT, new ShiftDetail("C101", OPENED_AT, CLOSED_AT, 1, BigDecimal.ONE, null), cash),
				"closed shift has no cash_counted");
		assertInvalid(shift(SHIFT, null, cash), "closed shift has no shift fields");
	}

	private static ShiftDetail detail(String cashierId, OffsetDateTime closedAt, String cashExpected, String cashCounted) {
		return new ShiftDetail(cashierId, OPENED_AT, closedAt, 212, new BigDecimal(cashExpected),
				new BigDecimal(cashCounted));
	}

	private static ConfirmedDay shift(ShiftKey key, ShiftDetail detail, Line... lines) {
		return new ConfirmedDay(RecordType.SHIFT_CLOSE, "BR0001", key, 1, CLOSED_AT, detail, List.of(lines));
	}
}
