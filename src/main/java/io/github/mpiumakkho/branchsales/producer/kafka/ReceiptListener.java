package io.github.mpiumakkho.branchsales.producer.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import io.github.mpiumakkho.branchsales.producer.config.ProducerProperties;
import io.github.mpiumakkho.branchsales.producer.dto.HqReceipt;
import io.github.mpiumakkho.branchsales.producer.dto.RecordType;
import io.github.mpiumakkho.branchsales.producer.repository.SyncStateStore;

/**
 * Reads HQ's receipts from the branch's own Kafka and records them in the send state.
 * <p>
 * HQ can answer within milliseconds, before the send round has stored the offset of what it sent. A receipt that
 * matches nothing is therefore retried for a short time ({@code KafkaConfig}) and only then logged and skipped.
 * A receipt that is not readable is HQ's error; it is logged and skipped.
 */
@Component
public class ReceiptListener {

	private static final Logger log = LoggerFactory.getLogger(ReceiptListener.class);

	private final JsonMapper mapper = JsonMapper.builder().build();
	private final SyncStateStore states;
	private final String branchCode;

	public ReceiptListener(SyncStateStore states, ProducerProperties properties) {
		this.states = states;
		this.branchCode = properties.branchCode();
	}

	@KafkaListener(topics = "${branch-sales.receipt-topic}", groupId = "${branch-sales.receipt-group}")
	public void onReceipt(ConsumerRecord<String, byte[]> record) {
		HqReceipt receipt;
		try {
			receipt = parse(record.value());
		}
		catch (JacksonException | IllegalArgumentException e) {
			log.error("Unreadable HQ receipt at offset {} skipped: {}", record.offset(), e.getMessage());
			return;
		}
		if (!branchCode.equals(receipt.branchCode())) {
			log.error("HQ receipt at offset {} is for branch {}, not {}; skipped", record.offset(),
					receipt.branchCode(), branchCode);
			return;
		}
		if (!states.applyReceipt(receipt)) {
			throw new UnmatchedReceiptException(receipt);
		}
		if (receipt.accepted()) {
			log.info("HQ {} the message at offset {} (stored revision {})", receipt.outcome(), receipt.sourceOffset(),
					receipt.storedRevision());
		}
		else {
			log.warn("HQ REJECTED the message at offset {}: {}", receipt.sourceOffset(), receipt.detail());
		}
	}

	private HqReceipt parse(byte[] value) {
		JsonNode root = mapper.readTree(value);
		String outcome = required(root, "outcome").asString();
		boolean rejected = "REJECTED".equals(outcome);
		// Receipts written before the return topic existed have no type: they are for daily sales
		JsonNode type = root.get("type");
		return new HqReceipt(
				type == null || type.isNull() ? RecordType.DAILY_SUMMARY : RecordType.valueOf(type.asString()),
				required(root, "branchCode").asString(),
				required(root, "sourceOffset").asLong(),
				outcome,
				rejected ? null : required(root, "storedRevision").asInt(),
				rejected ? required(root, "rejectReason").asString() : null,
				rejected ? required(root, "detail").asString() : null);
	}

	private static JsonNode required(JsonNode root, String field) {
		JsonNode node = root.get(field);
		if (node == null || node.isNull()) {
			throw new IllegalArgumentException("missing " + field);
		}
		return node;
	}

	static class UnmatchedReceiptException extends RuntimeException {

		UnmatchedReceiptException(HqReceipt receipt) {
			super("no " + receipt.type() + " revision was sent at offset " + receipt.sourceOffset() + " (HQ outcome "
					+ receipt.outcome() + ")");
		}
	}
}
