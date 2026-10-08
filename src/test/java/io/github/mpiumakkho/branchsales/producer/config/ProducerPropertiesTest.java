package io.github.mpiumakkho.branchsales.producer.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import io.github.mpiumakkho.branchsales.producer.dto.RecordType;

class ProducerPropertiesTest {

	private static final ProducerProperties.Schedule SCHEDULE = new ProducerProperties.Schedule("-", Duration.ZERO, "-");
	private static final String SUMMARY_TOPIC = "branch-sales.daily-summary";
	private static final String RETURN_TOPIC = "branch-sales.daily-return";
	private static final String SHIFT_TOPIC = "branch-sales.shift-close";
	private static final String RECEIPT_TOPIC = "branch-sales.receipt";

	@Test
	void acceptsValidConfiguration() {
		assertThatCode(() -> properties("BR0001", Map.of("BEV", "BEVERAGE", "X1", "PERSONAL_CARE")))
				.doesNotThrowAnyException();
	}

	@ParameterizedTest
	@NullAndEmptySource
	@ValueSource(strings = { "br0001", "BR 01", "BR0001 ", "B1", "BR000000001" })
	void rejectsBranchCodeOutsideContractPattern(String branchCode) {
		assertThatThrownBy(() -> properties(branchCode, Map.of()))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageStartingWith("branch-sales.branch-code must match");
	}

	@Test
	void rejectsMappingToCodeOutsideContractPattern() {
		assertThatThrownBy(() -> properties("BR0001", Map.of("BEV", "beverage", "SNK", "SNACK", "X", "DRINK-HOT")))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessage("branch-sales.category-mapping values must match ^[A-Z_]{2,30}$, got: {BEV=beverage, X=DRINK-HOT}");
	}

	@Test
	void rejectsLookbackOrResendAfterThatIsNotPositive() {
		assertThatThrownBy(() -> properties("BR0001", Map.of(), Duration.ZERO, Duration.ofHours(24)))
				.hasMessage("branch-sales.lookback and branch-sales.resend-after must be positive");
		assertThatThrownBy(() -> properties("BR0001", Map.of(), Duration.ofDays(60), Duration.ofHours(-1)))
				.hasMessage("branch-sales.lookback and branch-sales.resend-after must be positive");
	}

	private static ProducerProperties properties(String branchCode, Map<String, String> mapping) {
		return properties(branchCode, mapping, Duration.ofDays(60), Duration.ofHours(24));
	}

	@Test
	void rejectsRetentionNotLongerThanLookback() {
		assertThatThrownBy(() -> new ProducerProperties("BR0001", SUMMARY_TOPIC, RETURN_TOPIC, SHIFT_TOPIC, RECEIPT_TOPIC,
				Map.of(), Map.of(), SCHEDULE, Duration.ofSeconds(1), Duration.ofDays(60), Duration.ofHours(24), Duration.ofDays(60)))
				.hasMessage("branch-sales.retention (PT1440H) must be longer than branch-sales.lookback (PT1440H)");
	}

	private static ProducerProperties properties(String branchCode, Map<String, String> mapping, Duration lookback,
			Duration resendAfter) {
		return new ProducerProperties(branchCode, SUMMARY_TOPIC, RETURN_TOPIC, SHIFT_TOPIC, RECEIPT_TOPIC, mapping, Map.of(),
				SCHEDULE, Duration.ofSeconds(1), lookback, resendAfter, Duration.ofDays(90));
	}

	@Test
	void rejectsTenderMappingToCodeOutsideContractPattern() {
		assertThatThrownBy(() -> withTenders(Map.of("CSH", "CASH", "QR", "qr-payment", "GV", "GIFT VOUCHER")))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessage("branch-sales.tender-mapping values must match ^[A-Z_]{2,30}$, got: {GV=GIFT VOUCHER, QR=qr-payment}");
	}

	@Test
	void acceptsEmptyTenderMapping() {
		assertThat(withTenders(null).tenderMapping()).isEmpty();
		assertThat(withTenders(Map.of()).tenderMapping()).isEmpty();
		assertThat(withTenders(Map.of("CSH", "CASH", "CASH2", "CASH")).tenderMapping())
				.containsEntry("CSH", "CASH").containsEntry("CASH2", "CASH");
	}

	@ParameterizedTest
	@CsvSource({
			"same, same, other",
			"same, other, same",
			"other, same, same" })
	void rejectsAnyTwoEqualRecordTopics(String summary, String returns, String shifts) {
		assertThatThrownBy(() -> new ProducerProperties("BR0001", summary, returns, shifts, RECEIPT_TOPIC, Map.of(), Map.of(),
				SCHEDULE, Duration.ofSeconds(1), Duration.ofDays(60), Duration.ofHours(24), Duration.ofDays(90)))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessage("branch-sales.topic, branch-sales.return-topic and branch-sales.shift-close-topic must differ");
	}

	@ParameterizedTest
	@EnumSource(RecordType.class)
	void topicOfCoversEveryRecordType(RecordType type) {
		String expected = switch (type) {
			case DAILY_SUMMARY -> SUMMARY_TOPIC;
			case DAILY_RETURN -> RETURN_TOPIC;
			case SHIFT_CLOSE -> SHIFT_TOPIC;
		};
		assertThat(properties("BR0001", Map.of()).topicOf(type)).isEqualTo(expected);
	}

	private static ProducerProperties withTenders(Map<String, String> tenderMapping) {
		return new ProducerProperties("BR0001", SUMMARY_TOPIC, RETURN_TOPIC, SHIFT_TOPIC, RECEIPT_TOPIC, Map.of(),
				tenderMapping, SCHEDULE, Duration.ofSeconds(1), Duration.ofDays(60), Duration.ofHours(24), Duration.ofDays(90));
	}
}
