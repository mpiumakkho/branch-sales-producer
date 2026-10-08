package io.github.mpiumakkho.branchsales.producer.service;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;

import org.jspecify.annotations.Nullable;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

import io.github.mpiumakkho.branchsales.producer.config.ProducerProperties;
import io.github.mpiumakkho.branchsales.producer.dto.ConfirmedDay;
import io.github.mpiumakkho.branchsales.producer.dto.ConfirmedDay.ShiftDetail;
import io.github.mpiumakkho.branchsales.producer.dto.RecordKey.ShiftKey;
import io.github.mpiumakkho.branchsales.producer.dto.RecordType.LineKind;
import io.github.mpiumakkho.branchsales.producer.exception.InvalidSalesException;

/**
 * Writes a {@code DailySalesSummary}, {@code DailyReturn} or {@code ShiftClose} message (contract v1) from back-office
 * data. The two daily types differ only in the name of the date field. A shift close adds the terminal and shift to
 * the key and the shift's header fields, and has tender lines instead of category lines.
 */
@Component
public class DailyMessageWriter {

	/** Rule R7: business dates and times are Asia/Bangkok. */
	static final ZoneId BANGKOK = ZoneId.of("Asia/Bangkok");

	// Always prints seconds (OffsetDateTime.toString() leaves them out when zero, which RFC 3339 does not allow)
	private static final DateTimeFormatter CONFIRMED_AT = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

	// Limit from the contract schemas (line counts: RecordType.LineKind)
	private static final BigDecimal MAX_AMOUNT = new BigDecimal("999999999999.99");

	private final JsonMapper mapper = JsonMapper.builder().build();
	private final String branchCode;
	private final LineMapping categories;
	private final LineMapping tenders;

	public DailyMessageWriter(ProducerProperties properties) {
		this.branchCode = properties.branchCode();
		this.categories = new LineMapping(properties.categoryMapping(), "category", "categories");
		this.tenders = new LineMapping(properties.tenderMapping(), "tender", "tenders");
	}

	public record Message(UUID eventId, String key, byte[] value) {
	}

	/**
	 * @throws InvalidSalesException if the record cannot be written as a valid message
	 */
	public Message write(ConfirmedDay day) {
		// The branch identity comes from configuration. A back-office row with another code (re-coded branch, database
		// copied from another branch) would otherwise create a second HQ row for the same day under that code.
		check(branchCode.equals(day.branchCode()), day.type().table() + ".branch_code '" + day.branchCode()
				+ "' does not match configured branch code " + branchCode);
		ShiftKey shiftKey = day.key() instanceof ShiftKey k ? k : null;
		ShiftDetail shift = shiftKey == null ? null : checkedShift(shiftKey, day.shift());
		check(day.confirmedAt() != null, "confirmed day has no confirmed_at");
		LineKind lineKind = day.type().lineKind();
		Map<String, HqLine> lines = toHqLines(lineKind, lineKind.equals(LineKind.TENDER) ? tenders : categories,
				day.lines());
		BigDecimal total = lines.values().stream().map(HqLine::amount).reduce(BigDecimal.ZERO, BigDecimal::add);
		check(total.compareTo(MAX_AMOUNT) <= 0, "totalAmount " + total + " exceeds the contract maximum");

		UUID eventId = UUID.randomUUID();
		ObjectNode root = mapper.createObjectNode();
		root.put("schemaVersion", 1);
		root.put("eventId", eventId.toString());
		root.put("branchCode", branchCode);
		root.put(day.type().dateField(), day.date().toString());
		if (shiftKey != null) {
			root.put("terminalId", shiftKey.terminalId());
			root.put("shiftNo", shiftKey.shiftNo());
		}
		root.put("revision", day.revision());
		root.put("confirmedAt", time(day.confirmedAt()));
		if (shift != null) {
			if (shift.cashierId() != null) {
				root.put("cashierId", shift.cashierId());
			}
			root.put("openedAt", time(shift.openedAt()));
			root.put("closedAt", time(shift.closedAt()));
			root.put("transactionCount", shift.transactionCount());
		}
		root.put("currency", "THB");
		root.put("totalAmount", money(total));
		if (shift != null) {
			root.put("cashExpected", money(shift.cashExpected()));
			root.put("cashCounted", money(shift.cashCounted()));
		}
		ArrayNode array = root.putArray("lines");
		lines.forEach((code, line) -> array.addObject()
				.put(lineKind.codeField(), code)
				.put("amount", money(line.amount()))
				.put("quantity", line.quantity()));

		// Key = branchCode: all records of one branch go to the same partition of their topic, in order
		return new Message(eventId, branchCode, mapper.writeValueAsBytes(root));
	}

	/** Checks what the shift close schema requires and the back-office columns do not guarantee. */
	private static ShiftDetail checkedShift(ShiftKey key, @Nullable ShiftDetail shift) {
		// terminal_id is also a segment of the sync_state id, which is why # is not allowed
		check(ProducerProperties.TERMINAL_ID.matcher(key.terminalId()).matches(), "pos_shift.terminal_id '"
				+ key.terminalId() + "' does not match " + ProducerProperties.TERMINAL_ID);
		check(key.shiftNo() >= 1, "pos_shift.shift_no " + key.shiftNo() + " is not positive");
		if (shift == null) {
			throw new InvalidSalesException("closed shift has no shift fields");
		}
		check(shift.cashierId() == null || ProducerProperties.CASHIER_ID.matcher(shift.cashierId()).matches(),
				"pos_shift.cashier_id '" + shift.cashierId() + "' does not match " + ProducerProperties.CASHIER_ID);
		check(shift.openedAt() != null, "closed shift has no opened_at");
		check(shift.closedAt() != null, "closed shift has no closed_at");
		check(!shift.closedAt().isBefore(shift.openedAt()),
				"closed_at " + shift.closedAt() + " is before opened_at " + shift.openedAt());
		check(shift.transactionCount() >= 0, "negative transaction_count " + shift.transactionCount());
		checkCash("cash_expected", shift.cashExpected());
		checkCash("cash_counted", shift.cashCounted());
		return shift;
	}

	private static void checkCash(String column, @Nullable BigDecimal amount) {
		check(amount != null, "closed shift has no " + column);
		check(amount.signum() >= 0, "negative " + column + " " + amount);
		check(amount.stripTrailingZeros().scale() <= 2, column + " with more than 2 decimals: " + amount);
		check(amount.compareTo(MAX_AMOUNT) <= 0, column + " " + amount + " exceeds the contract maximum");
	}

	/** Maps local codes to HQ codes. Local codes that map to the same HQ code are added up into one line. */
	private static Map<String, HqLine> toHqLines(LineKind kind, LineMapping mapping, List<ConfirmedDay.Line> localLines) {
		check(localLines.size() >= kind.minLines(), "confirmed day has no lines");
		var unmapped = new TreeSet<String>();
		var lines = new TreeMap<String, HqLine>();
		for (ConfirmedDay.Line local : localLines) {
			check(local.amount().signum() >= 0, "negative amount for " + mapping.noun() + " " + local.localCode());
			check(local.quantity() >= 0, "negative quantity for " + mapping.noun() + " " + local.localCode());
			check(local.amount().stripTrailingZeros().scale() <= 2,
					"amount with more than 2 decimals for " + mapping.noun() + " " + local.localCode());
			String hqCode = mapping.codes().get(local.localCode());
			if (hqCode == null) {
				unmapped.add(local.localCode());
				continue;
			}
			lines.merge(hqCode, new HqLine(local.amount(), local.quantity()), HqLine::plus);
		}
		check(unmapped.isEmpty(), "no HQ " + mapping.noun() + " mapping for local " + mapping.noun() + " " + unmapped);
		check(lines.size() <= kind.maxLines(),
				lines.size() + " " + mapping.plural() + ", the contract allows " + kind.maxLines());
		return lines;
	}

	private static String time(OffsetDateTime time) {
		return CONFIRMED_AT.format(time.atZoneSameInstant(BANGKOK));
	}

	private static String money(BigDecimal amount) {
		return amount.setScale(2).toPlainString();
	}

	private static void check(boolean condition, String message) {
		if (!condition) {
			throw new InvalidSalesException(message);
		}
	}

	/** Local code to HQ code, and how the code is named in error messages. */
	private record LineMapping(Map<String, String> codes, String noun, String plural) {
	}

	private record HqLine(BigDecimal amount, long quantity) {

		HqLine plus(HqLine other) {
			return new HqLine(amount.add(other.amount), quantity + other.quantity);
		}
	}
}
