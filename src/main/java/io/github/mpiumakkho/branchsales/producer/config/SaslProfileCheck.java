package io.github.mpiumakkho.branchsales.producer.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * Profile "sasl" puts {@code KAFKA_PASSWORD} into the SCRAM login (application-sasl.yaml). If it is not set, the
 * placeholder would be sent as the password and every send would fail at the broker; stop at startup instead.
 */
@Configuration(proxyBeanMethods = false)
@Profile("sasl")
class SaslProfileCheck {

	SaslProfileCheck(@Value("${KAFKA_PASSWORD:}") String password) {
		if (password.isBlank()) {
			throw new IllegalStateException("profile sasl: set KAFKA_PASSWORD to the branch's Kafka (SCRAM) password");
		}
	}
}
