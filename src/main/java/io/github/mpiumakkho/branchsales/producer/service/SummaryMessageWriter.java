package io.github.mpiumakkho.branchsales.producer.service;

import java.math.BigDecimal;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;

import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

import io.github.mpiumakkho.branchsales.producer.config.ProducerProperties;
import io.github.mpiumakkho.branchsales.producer.dto.ConfirmedSales;
import io.github.mpiumakkho.branchsales.producer.exception.InvalidSalesException;

/**
 * Writes a {@code DailySalesSummary} message (contract v1) from back-office data.
 */
@Component
public class SummaryMessageWriter {

	/** Rule R7: business dates and times are Asia/Bangkok. */
	static final ZoneId BANGKOK = ZoneId.of("Asia/Bangkok");

	// Always prints seconds (OffsetDateTime.toString() leaves them out when zero, which RFC 3339 does not allow)
	private static final DateTimeFormatter CONFIRMED_AT = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

	// Limits from the contract schema
	private static final int MAX_LINES = 50;
	private static final BigDecimal MAX_AMOUNT = new BigDecimal("999999999999.99");

	private final JsonMapper mapper = JsonMapper.builder().build();
	private final String branchCode;
	private final Map<String, String> categoryMapping;

	public SummaryMessageWriter(ProducerProperties properties) {
		this.branchCode = properties.branchCode();
		this.categoryMapping = properties.categoryMapping();
	}

	public record Message(UUID eventId, String key, byte[] value) {
	}

	/**
	 * @throws InvalidSalesException if the day cannot be written as a valid message
	 */
	public Message write(ConfirmedSales sales) {
		// The branch identity comes from configuration. A back-office row with another code (re-coded branch, database
		// copied from another branch) would otherwise create a second HQ row for the same day under that code.
		check(branchCode.equals(sales.branchCode()),
				"daily_sales.branch_code '" + sales.branchCode() + "' does not match configured branch code " + branchCode);
		check(sales.confirmedAt() != null, "confirmed day has no confirmed_at");
		Map<String, HqLine> lines = toHqLines(sales.lines());
		BigDecimal total = lines.values().stream().map(HqLine::amount).reduce(BigDecimal.ZERO, BigDecimal::add);
		check(total.compareTo(MAX_AMOUNT) <= 0, "totalAmount " + total + " exceeds the contract maximum");

		UUID eventId = UUID.randomUUID();
		ObjectNode root = mapper.createObjectNode();
		root.put("schemaVersion", 1);
		root.put("eventId", eventId.toString());
		root.put("branchCode", branchCode);
		root.put("saleDate", sales.saleDate().toString());
		root.put("revision", sales.revision());
		root.put("confirmedAt", CONFIRMED_AT.format(sales.confirmedAt().atZoneSameInstant(BANGKOK)));
		root.put("currency", "THB");
		root.put("totalAmount", money(total));
		ArrayNode array = root.putArray("lines");
		lines.forEach((code, line) -> array.addObject()
				.put("categoryCode", code)
				.put("amount", money(line.amount()))
				.put("quantity", line.quantity()));

		// Key = branchCode: all days of one branch go to the same partition, in order
		return new Message(eventId, branchCode, mapper.writeValueAsBytes(root));
	}

	/** Maps local codes to HQ codes. Local codes that map to the same HQ code are added up into one line. */
	private Map<String, HqLine> toHqLines(List<ConfirmedSales.Line> localLines) {
		check(!localLines.isEmpty(), "confirmed day has no lines");
		var unmapped = new TreeSet<String>();
		var lines = new TreeMap<String, HqLine>();
		for (ConfirmedSales.Line local : localLines) {
			check(local.amount().signum() >= 0, "negative amount for category " + local.localCategoryCode());
			check(local.quantity() >= 0, "negative quantity for category " + local.localCategoryCode());
			check(local.amount().stripTrailingZeros().scale() <= 2,
					"amount with more than 2 decimals for category " + local.localCategoryCode());
			String hqCode = categoryMapping.get(local.localCategoryCode());
			if (hqCode == null) {
				unmapped.add(local.localCategoryCode());
				continue;
			}
			lines.merge(hqCode, new HqLine(local.amount(), local.quantity()), HqLine::plus);
		}
		check(unmapped.isEmpty(), "no HQ category mapping for local category " + unmapped);
		check(lines.size() <= MAX_LINES, lines.size() + " categories, the contract allows " + MAX_LINES);
		return lines;
	}

	private static String money(BigDecimal amount) {
		return amount.setScale(2).toPlainString();
	}

	private static void check(boolean condition, String message) {
		if (!condition) {
			throw new InvalidSalesException(message);
		}
	}

	private record HqLine(BigDecimal amount, long quantity) {

		HqLine plus(HqLine other) {
			return new HqLine(amount.add(other.amount), quantity + other.quantity);
		}
	}
}
