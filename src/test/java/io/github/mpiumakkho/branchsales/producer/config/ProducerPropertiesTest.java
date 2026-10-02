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

	private static ProducerProperties properties(String branchCode, Map<String, String> mapping) {
		return new ProducerProperties(branchCode, "branch-sales.daily-summary", mapping, SCHEDULE, Duration.ofSeconds(1));
	}
}
