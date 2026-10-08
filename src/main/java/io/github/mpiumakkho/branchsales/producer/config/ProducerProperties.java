package io.github.mpiumakkho.branchsales.producer.config;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import io.github.mpiumakkho.branchsales.producer.dto.RecordType;

/**
 * Checked when the application starts: a wrong branch code or mapping stops the producer instead of sending
 * messages that HQ would reject.
 *
 * @param branchCode      this branch's code in the HQ branch registry. Only back-office days with this
 *                        {@code branch_code} are sent; it is also the record key and {@code branchCode} of every message
 * @param topic           daily-summary topic in the branch's own Kafka, read by HQ (contract/README.md)
 * @param returnTopic     daily-return topic in the branch's own Kafka, read by HQ
 * @param shiftCloseTopic shift-close topic in the branch's own Kafka, read by HQ
 * @param receiptTopic    receipt topic in the branch's own Kafka, written by HQ
 * @param categoryMapping back-office category code to HQ category code; several local codes may map to one HQ code
 * @param tenderMapping   POS tender code to HQ tender type, with the same rules as {@code categoryMapping}
 * @param schedule        when send rounds start
 * @param sendTimeout     how long to wait for the broker ack of one message
 * @param lookback        how far back confirmed days are read; older days are not sent again
 * @param resendAfter     a day that was sent but has no HQ receipt after this long is sent again
 * @param retention       send state of days HQ accepted is deleted this long after its last change (Q9). Must be
 *                        longer than {@code lookback}, otherwise an accepted day still in the lookback would be sent
 *                        again
 */
@ConfigurationProperties("branch-sales")
public record ProducerProperties(
		String branchCode,
		@DefaultValue("branch-sales.daily-summary") String topic,
		@DefaultValue("branch-sales.daily-return") String returnTopic,
		@DefaultValue("branch-sales.shift-close") String shiftCloseTopic,
		@DefaultValue("branch-sales.receipt") String receiptTopic,
		Map<String, String> categoryMapping,
		Map<String, String> tenderMapping,
		Schedule schedule,
		@DefaultValue("60s") Duration sendTimeout,
		@DefaultValue("60d") Duration lookback,
		@DefaultValue("24h") Duration resendAfter,
		@DefaultValue("90d") Duration retention) {

	// Patterns from the contract schemas (contract/daily-sales-summary.v1.schema.json, shift-close.v1.schema.json)
	static final Pattern BRANCH_CODE = Pattern.compile("^[A-Z0-9]{3,10}$");
	/** HQ category code and HQ tender type. */
	static final Pattern HQ_CODE = Pattern.compile("^[A-Z_]{2,30}$");
	/** POS terminal id of a shift close; it is also a segment of the sync_state id, so it never contains {@code #}. */
	public static final Pattern TERMINAL_ID = Pattern.compile("^[A-Z0-9_-]{1,20}$");
	/** POS login id of a shift close's cashier. */
	public static final Pattern CASHIER_ID = Pattern.compile("^[A-Za-z0-9_-]{1,30}$");

	public ProducerProperties {
		if (branchCode == null || !BRANCH_CODE.matcher(branchCode).matches()) {
			throw new IllegalArgumentException("branch-sales.branch-code must match " + BRANCH_CODE + ", got: " + branchCode);
		}
		categoryMapping = checkedMapping("branch-sales.category-mapping", categoryMapping);
		tenderMapping = checkedMapping("branch-sales.tender-mapping", tenderMapping);
		if (lookback.isNegative() || lookback.isZero() || resendAfter.isNegative() || resendAfter.isZero()) {
			throw new IllegalArgumentException("branch-sales.lookback and branch-sales.resend-after must be positive");
		}
		if (retention.compareTo(lookback) <= 0) {
			throw new IllegalArgumentException("branch-sales.retention (" + retention
					+ ") must be longer than branch-sales.lookback (" + lookback + ")");
		}
		// Not Set.of, which throws on duplicates itself
		if (new HashSet<>(List.of(topic, returnTopic, shiftCloseTopic)).size() != 3) {
			throw new IllegalArgumentException(
					"branch-sales.topic, branch-sales.return-topic and branch-sales.shift-close-topic must differ");
		}
	}

	private static Map<String, String> checkedMapping(String name, Map<String, String> mapping) {
		Map<String, String> copy = mapping == null ? Map.of() : Map.copyOf(mapping);
		var invalid = new TreeMap<String, String>();
		copy.forEach((local, hq) -> {
			if (!HQ_CODE.matcher(hq).matches()) {
				invalid.put(local, hq);
			}
		});
		if (!invalid.isEmpty()) {
			throw new IllegalArgumentException(name + " values must match " + HQ_CODE + ", got: " + invalid);
		}
		return copy;
	}

	/** The topic a record type is sent to. */
	public String topicOf(RecordType type) {
		return switch (type) {
			case DAILY_SUMMARY -> topic;
			case DAILY_RETURN -> returnTopic;
			case SHIFT_CLOSE -> shiftCloseTopic;
		};
	}

	/**
	 * @param cron        round start times, Asia/Bangkok
	 * @param maxJitter   each round waits a random time between 0 and this before reading the database, so branches
	 *                    do not all send at the same moment
	 * @param cleanupCron when accepted send state older than {@code retention} is deleted, Asia/Bangkok
	 */
	public record Schedule(String cron, Duration maxJitter, @DefaultValue("0 30 3 * * *") String cleanupCron) {
	}
}
