package io.github.mpiumakkho.branchsales.producer.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration(proxyBeanMethods = false)
class KafkaConfig {

	/**
	 * Error handler of the receipt listener (picked up by Spring Boot's listener container factory). A receipt that
	 * matches no sent revision yet is tried again for about 10 seconds: HQ may answer before the send round has
	 * recorded the offset. After that it is logged and skipped; the revision is then sent again after resend-after,
	 * and HQ answers DUPLICATE.
	 */
	@Bean
	DefaultErrorHandler receiptErrorHandler() {
		return new DefaultErrorHandler(new FixedBackOff(1_000, 10));
	}
}
