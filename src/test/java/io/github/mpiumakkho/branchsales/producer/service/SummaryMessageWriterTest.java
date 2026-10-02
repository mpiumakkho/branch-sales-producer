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

import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import io.github.mpiumakkho.branchsales.producer.ContractSchema;
import io.github.mpiumakkho.branchsales.producer.config.ProducerProperties;
import io.github.mpiumakkho.branchsales.producer.dto.ConfirmedSales;
import io.github.mpiumakkho.branchsales.producer.dto.ConfirmedSales.Line;
import io.github.mpiumakkho.branchsales.producer.exception.InvalidSalesException;

class SummaryMessageWriterTest {

	private static final Map<String, String> MAPPING = Map.of(
			"BEV", "BEVERAGE",
			"DRINK_HOT", "BEVERAGE",
			"SNK", "SNACK",
			"RTE", "READY_MEAL");

	private final SummaryMessageWriter writer = new SummaryMessageWriter(
			new ProducerProperties("branch-sales.daily-summary", MAPPING,
					new ProducerProperties.Schedule("-", Duration.ZERO), Duration.ofSeconds(1)));

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
		var message = writer.write(new ConfirmedSales("BR0001", LocalDate.of(2026, 10, 1), 1,
				OffsetDateTime.parse("2026-10-01T14:45:00Z"), List.of(new Line("BEV", BigDecimal.TEN, 1))));

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
		assertInvalid(new ConfirmedSales("BR0001", LocalDate.of(2026, 10, 1), 1, null,
				List.of(new Line("BEV", BigDecimal.ONE, 1))), "confirmed day has no confirmed_at");
	}

	private void assertInvalid(ConfirmedSales sales, String message) {
		assertThatThrownBy(() -> writer.write(sales)).isInstanceOf(InvalidSalesException.class).hasMessage(message);
	}

	private static ConfirmedSales sales(Line... lines) {
		return new ConfirmedSales("BR0001", LocalDate.of(2026, 10, 1), 2,
				OffsetDateTime.parse("2026-10-01T21:45:00+07:00"), List.of(lines));
	}
}
