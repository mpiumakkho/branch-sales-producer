package io.github.mpiumakkho.branchsales.producer.config;

import java.time.Duration;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Checked when the application starts: a wrong branch code or mapping stops the producer instead of sending
 * messages that HQ would reject.
 *
 * @param branchCode      this branch's code in the HQ branch registry. Only back-office days with this
 *                        {@code branch_code} are sent; it is also the record key and {@code branchCode} of every message
 * @param topic           this branch's topic, {@code branch-sales.daily-summary.<branchCode>}: the only topic the
 *                        branch's Kafka user may write
 * @param categoryMapping back-office category code to HQ category code; several local codes may map to one HQ code
 * @param schedule        when send rounds start
 * @param sendTimeout     how long to wait for the broker ack of one message
 */
@ConfigurationProperties("branch-sales")
public record ProducerProperties(
		String branchCode,
		String topic,
		Map<String, String> categoryMapping,
		Schedule schedule,
		@DefaultValue("60s") Duration sendTimeout) {

	// Patterns from the contract schema (contract/daily-sales-summary.v1.schema.json)
	static final Pattern BRANCH_CODE = Pattern.compile("^[A-Z0-9]{3,10}$");
	static final Pattern HQ_CATEGORY_CODE = Pattern.compile("^[A-Z_]{2,30}$");
	static final String TOPIC_PREFIX = "branch-sales.daily-summary.";

	public ProducerProperties {
		if (branchCode == null || !BRANCH_CODE.matcher(branchCode).matches()) {
			throw new IllegalArgumentException("branch-sales.branch-code must match " + BRANCH_CODE + ", got: " + branchCode);
		}
		if (!(TOPIC_PREFIX + branchCode).equals(topic)) {
			// Any other topic is refused by the broker (ACL) or rejected by HQ (BRANCH_MISMATCH)
			throw new IllegalArgumentException("branch-sales.topic must be " + TOPIC_PREFIX + branchCode + ", got: " + topic);
		}
		categoryMapping = categoryMapping == null ? Map.of() : Map.copyOf(categoryMapping);
		var invalid = new TreeMap<String, String>();
		categoryMapping.forEach((local, hq) -> {
			if (!HQ_CATEGORY_CODE.matcher(hq).matches()) {
				invalid.put(local, hq);
			}
		});
		if (!invalid.isEmpty()) {
			throw new IllegalArgumentException(
					"branch-sales.category-mapping values must match " + HQ_CATEGORY_CODE + ", got: " + invalid);
		}
	}

	/**
	 * @param cron      round start times, Asia/Bangkok
	 * @param maxJitter each round waits a random time between 0 and this before reading the database, so branches
	 *                  do not all send at the same moment
	 */
	public record Schedule(String cron, Duration maxJitter) {
	}
}
