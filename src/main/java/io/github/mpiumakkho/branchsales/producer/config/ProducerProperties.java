package io.github.mpiumakkho.branchsales.producer.config;

import java.time.Duration;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * @param topic           contract topic for daily summaries
 * @param categoryMapping back-office category code to HQ category code; several local codes may map to one HQ code
 * @param schedule        when send rounds start
 * @param sendTimeout     how long to wait for the broker ack of one message
 */
@ConfigurationProperties("branch-sales")
public record ProducerProperties(
		String topic,
		Map<String, String> categoryMapping,
		Schedule schedule,
		@DefaultValue("60s") Duration sendTimeout) {

	public ProducerProperties {
		categoryMapping = categoryMapping == null ? Map.of() : Map.copyOf(categoryMapping);
	}

	/**
	 * @param cron      round start times, Asia/Bangkok
	 * @param maxJitter each round waits a random time between 0 and this before reading the database, so branches
	 *                  do not all send at the same moment
	 */
	public record Schedule(String cron, Duration maxJitter) {
	}
}
