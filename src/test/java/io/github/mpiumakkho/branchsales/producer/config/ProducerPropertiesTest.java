package io.github.mpiumakkho.branchsales.producer.config;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

class ProducerPropertiesTest {

	private static final ProducerProperties.Schedule SCHEDULE = new ProducerProperties.Schedule("-", Duration.ZERO);

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

	private static ProducerProperties properties(String branchCode, Map<String, String> mapping, Duration lookback,
			Duration resendAfter) {
		return new ProducerProperties(branchCode, "branch-sales.daily-summary", "branch-sales.receipt", mapping,
				SCHEDULE, Duration.ofSeconds(1), lookback, resendAfter);
	}
}
