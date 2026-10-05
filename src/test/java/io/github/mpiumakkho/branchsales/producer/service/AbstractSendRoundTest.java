package io.github.mpiumakkho.branchsales.producer.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.testcontainers.kafka.KafkaContainer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

import io.github.mpiumakkho.branchsales.producer.ContractSchema;
import io.github.mpiumakkho.branchsales.producer.dto.ConfirmedSales;
import io.github.mpiumakkho.branchsales.producer.kafka.SummaryPublisher;
import io.github.mpiumakkho.branchsales.producer.repository.ConfirmedSalesReader;
import io.github.mpiumakkho.branchsales.producer.repository.SyncStateStore;

/**
 * Runs send rounds against a branch database with the simulated back-office tables, the branch's Kafka broker and
 * MongoDB. HQ is simulated by writing receipts to the receipt topic. The same tests run once per supported database
 * (one subclass each). Rounds are started directly; the cron is off.
 */
@SpringBootTest(properties = {
		"branch-sales.schedule.cron=-",
		"branch-sales.branch-code=BR0001",
		"branch-sales.category-mapping.BEV=BEVERAGE",
		"branch-sales.category-mapping.SNK=SNACK",
		"spring.kafka.producer.properties.delivery.timeout.ms=15000",
		"spring.kafka.producer.properties.request.timeout.ms=5000" })
abstract class AbstractSendRoundTest {

	private static final Duration TIMEOUT = Duration.ofSeconds(30);
	private static final ZoneId BANGKOK = ZoneId.of("Asia/Bangkok");
	// Recent days, so they are within the 60-day lookback whenever the tests run
	private static final LocalDate DAY_1 = LocalDate.now(BANGKOK).minusDays(3);
	private static final LocalDate DAY_2 = DAY_1.plusDays(1);
	private static final OffsetDateTime CONFIRMED_AT = DAY_1.atTime(21, 45).atOffset(ZoneOffset.ofHours(7));

	@Autowired
	SendRound round;

	@Autowired
	ConfirmedSalesReader reader;

	@Autowired
	SyncStateStore states;

	@Autowired
	DataSource dataSource;

	@MockitoSpyBean
	SummaryPublisher publisher;

	@Autowired
	JdbcClient jdbc;

	@Autowired
	MongoTemplate mongo;

	@Autowired
	KafkaTemplate<String, byte[]> kafka;

	// Not ${spring.kafka.bootstrap-servers}: @ServiceConnection does not set that property, so it would resolve to
	// the application.yaml default (a local Kafka, if one is running) instead of the test container
	@Autowired
	KafkaContainer kafkaContainer;

	@Value("${branch-sales.topic}")
	String topic;

	@Value("${branch-sales.receipt-topic}")
	String receiptTopic;

	private final JsonMapper mapper = JsonMapper.builder().build();
	private KafkaConsumer<String, byte[]> topicReader;

	@BeforeEach
	void setUp() {
		mongo.remove(new Query(), "sync_state");
		jdbc.sql("delete from daily_sales_line").update();
		jdbc.sql("delete from daily_sales").update();
		topicReader = openTopicReaderAtEnd();
	}

	@AfterEach
	void tearDown() {
		topicReader.close();
	}

	@Test
	void sendsConfirmedDayOnceAndWaitsForHqReceipt() {
		insertDay(DAY_1, "CONFIRMED", 1);

		assertThat(round.run()).isEqualTo(new SendRound.Result(1, 1, 0));

		ConsumerRecord<String, byte[]> record = readMessages(1).getFirst();
		assertThat(record.key()).isEqualTo("BR0001");
		assertThat(ContractSchema.errors(record.value())).isEmpty();
		JsonNode json = mapper.readTree(record.value());
		assertThat(json.get("saleDate").asString()).isEqualTo(DAY_1.toString());
		assertThat(json.get("revision").intValue()).isEqualTo(1);
		assertThat(json.get("confirmedAt").asString()).isEqualTo(DAY_1 + "T21:45:00+07:00");
		assertThat(json.get("totalAmount").asString()).isEqualTo("30250.00");

		Document state = state(DAY_1, 1);
		assertThat(state).containsEntry("status", "SENT").containsEntry("attempts", 1)
				.containsEntry("eventId", json.get("eventId").asString())
				.containsEntry("sentOffsets", List.of(record.offset()))
				.doesNotContainKey("lastError");
		assertThat(state.get("sentAt")).isNotNull();
		assertThat(history(DAY_1, 1)).containsExactly("SENT offset=" + record.offset());

		// Waiting for HQ: not sent again
		assertThat(round.run()).isEqualTo(new SendRound.Result(0, 0, 0));

		// HQ stored it: final
		sendReceipt(record.offset(), "INSERTED", null);
		await().atMost(TIMEOUT).until(() -> "HQ_ACCEPTED".equals(state(DAY_1, 1).getString("status")));
		assertThat(state(DAY_1, 1)).containsEntry("hqOutcome", "INSERTED").containsEntry("hqStoredRevision", 1);
		assertThat(history(DAY_1, 1)).containsExactly("SENT offset=" + record.offset(), "HQ_INSERTED offset=" + record.offset());
		assertThat(round.run()).isEqualTo(new SendRound.Result(0, 0, 0));
		assertThat(topicReader.poll(Duration.ofSeconds(1))).isEmpty();
	}

	@Test
	void rejectedByHqIsNotSentAgainUntilReconfirmed() {
		insertDay(DAY_1, "CONFIRMED", 1);
		round.run();
		long offset = readMessages(1).getFirst().offset();

		sendReceipt(offset, "REJECTED", "UNKNOWN_CATEGORY");
		await().atMost(TIMEOUT).until(() -> "HQ_REJECTED".equals(state(DAY_1, 1).getString("status")));
		assertThat(state(DAY_1, 1)).containsEntry("hqRejectReason", "UNKNOWN_CATEGORY")
				.containsEntry("hqDetail", "UNKNOWN_CATEGORY: test");
		assertThat(round.run().pending()).isZero();

		// The manager fixes the day and confirms again: a new revision, sent in the next round
		confirm(DAY_1, 2);
		assertThat(round.run()).isEqualTo(new SendRound.Result(1, 1, 0));
		assertThat(state(DAY_1, 2)).containsEntry("status", "SENT");

		// HQ replays the rejected record later and stores it: revision 1 becomes accepted
		sendReceipt(offset, "INSERTED", null);
		await().atMost(TIMEOUT).until(() -> "HQ_ACCEPTED".equals(state(DAY_1, 1).getString("status")));
		assertThat(state(DAY_1, 1)).doesNotContainKeys("hqRejectReason", "hqDetail");
	}

	@Test
	void sentWithoutReceiptIsSentAgainAfterResendAfter() {
		insertDay(DAY_1, "CONFIRMED", 1);
		round.run();
		long firstOffset = readMessages(1).getFirst().offset();

		// 25 hours later, still no receipt (e.g. the branch broker lost the message)
		mongo.updateFirst(byId(DAY_1, 1), Update.update("sentAt", Date.from(Instant.now().minus(Duration.ofHours(25)))),
				"sync_state");
		assertThat(round.run()).isEqualTo(new SendRound.Result(1, 1, 0));
		long secondOffset = readMessages(1).getFirst().offset();
		assertThat(state(DAY_1, 1)).containsEntry("attempts", 2)
				.containsEntry("sentOffsets", List.of(firstOffset, secondOffset));

		// A receipt for either copy completes the revision; the other copy then gets DUPLICATE and changes nothing
		sendReceipt(firstOffset, "INSERTED", null);
		sendReceipt(secondOffset, "DUPLICATE", null);
		await().atMost(TIMEOUT).until(() -> history(DAY_1, 1).size() == 4);
		assertThat(state(DAY_1, 1)).containsEntry("status", "HQ_ACCEPTED").containsEntry("hqOutcome", "DUPLICATE");
	}

	@Test
	void receiptThatArrivesBeforeTheSentOffsetIsRecordedIsAppliedWhenItIs() {
		// HQ answered faster than the send round wrote SENT: the receipt waits (retried) until the offset is known
		sendReceipt(42, "INSERTED", null);
		try {
			Thread.sleep(1500);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		states.markSent(DAY_1, 1, UUID.randomUUID(), 42);
		await().atMost(TIMEOUT).until(() -> "HQ_ACCEPTED".equals(state(DAY_1, 1).getString("status")));
	}

	@Test
	void sendsOnlyConfirmedRevisions() {
		insertDay(DAY_1, "DRAFT", 0);
		assertThat(round.run().pending()).isZero(); // R1

		confirm(DAY_1, 1);
		round.run();
		// Manager edits after sending: back to DRAFT, nothing to send until confirmed again
		jdbc.sql("update daily_sales set status = 'DRAFT' where sale_date = ?").param(DAY_1).update();
		jdbc.sql("update daily_sales_line set amount = 18700.00 where sale_date = ? and category_code = 'BEV'")
				.param(DAY_1).update();
		assertThat(round.run().pending()).isZero();

		confirm(DAY_1, 2); // R3
		assertThat(round.run()).isEqualTo(new SendRound.Result(1, 1, 0));

		List<ConsumerRecord<String, byte[]>> records = readMessages(2);
		JsonNode revision2 = mapper.readTree(records.get(1).value());
		assertThat(revision2.get("revision").intValue()).isEqualTo(2);
		assertThat(revision2.get("totalAmount").asString()).isEqualTo("30750.00");
		assertThat(state(DAY_1, 1)).containsEntry("status", "SENT");
		assertThat(state(DAY_1, 2)).containsEntry("status", "SENT");
	}

	@Test
	void daysOlderThanTheLookbackAreNotRead() {
		insertDay(LocalDate.now(BANGKOK).minusDays(61), "CONFIRMED", 1);
		assertThat(round.run().pending()).isZero();
	}

	@Test
	void dayWithUnmappedCategoryIsRecordedFailedAndOtherDaysAreStillSent() {
		insertDay(DAY_1, "CONFIRMED", 1);
		jdbc.sql("insert into daily_sales_line values (?, 'LOTTO', 500.00, 5)").param(DAY_1).update();
		insertDay(DAY_2, "CONFIRMED", 1);

		assertThat(round.run()).isEqualTo(new SendRound.Result(2, 1, 1));

		assertThat(mapper.readTree(readMessages(1).getFirst().value()).get("saleDate").asString())
				.isEqualTo(DAY_2.toString());
		assertThat(state(DAY_1, 1)).containsEntry("status", "FAILED").containsEntry("attempts", 1)
				.containsEntry("lastError", "no HQ category mapping for local category [LOTTO]");
		assertThat(state(DAY_2, 1)).containsEntry("status", "SENT");

		// Still pending: tried again every round until the data or the mapping is fixed
		assertThat(round.run()).isEqualTo(new SendRound.Result(1, 0, 1));
		assertThat(state(DAY_1, 1)).containsEntry("attempts", 2);
		assertThat(history(DAY_1, 1)).containsExactly(
				"FAILED error=no HQ category mapping for local category [LOTTO]",
				"FAILED error=no HQ category mapping for local category [LOTTO]");
	}

	@Test
	void readingIsNotBlockedByAnOpenBackOfficeTransaction() throws Exception {
		insertDay(DAY_1, "CONFIRMED", 1);

		// The manager starts editing the day; the back-office transaction is still open (row locked)
		try (Connection backOffice = dataSource.getConnection()) {
			backOffice.setAutoCommit(false);
			try (PreparedStatement edit = backOffice.prepareStatement(
					"update daily_sales set status = 'DRAFT' where sale_date = ?")) {
				edit.setObject(1, DAY_1);
				edit.executeUpdate();
			}
			try {
				// Reads the last committed state from its snapshot instead of waiting for the lock (on SQL Server
				// only with SNAPSHOT isolation; its REPEATABLE READ would wait here)
				List<ConfirmedSales> confirmed = CompletableFuture.supplyAsync(() -> reader.readConfirmed(DAY_1))
						.get(5, TimeUnit.SECONDS);
				assertThat(confirmed).extracting(ConfirmedSales::saleDate).containsExactly(DAY_1);
			}
			finally {
				backOffice.rollback();
			}
		}
	}

	@Test
	void dayWithAnotherBranchCodeIsNotSent() {
		insertDay(DAY_1, "CONFIRMED", 1);
		jdbc.sql("update daily_sales set branch_code = 'BR0009' where sale_date = ?").param(DAY_1).update();

		assertThat(round.run()).isEqualTo(new SendRound.Result(1, 0, 1));
		assertThat(state(DAY_1, 1)).containsEntry("status", "FAILED")
				.containsEntry("lastError", "daily_sales.branch_code 'BR0009' does not match configured branch code BR0001");
		assertThat(topicReader.poll(Duration.ofSeconds(1))).isEmpty();
	}

	@Test
	void kafkaFailureStopsTheRoundAndTheNextRoundSendsEverything() {
		insertDay(DAY_1, "CONFIRMED", 1);
		insertDay(DAY_2, "CONFIRMED", 1);
		doThrow(new SummaryPublisher.PublishException(new RuntimeException("simulated: broker not reachable")))
				.doCallRealMethod()
				.when(publisher).publish(any());

		assertThat(round.run()).isEqualTo(new SendRound.Result(2, 0, 1));
		assertThat(state(DAY_1, 1)).containsEntry("status", "FAILED")
				.extractingByKey("lastError").asString().contains("simulated: broker not reachable");
		assertThat(state(DAY_2, 1)).isNull(); // not attempted
		assertThat(topicReader.poll(Duration.ofSeconds(1))).isEmpty();

		assertThat(round.run()).isEqualTo(new SendRound.Result(2, 2, 0));
		List<ConsumerRecord<String, byte[]>> records = readMessages(2);
		// Oldest day first, so HQ reads them in order
		assertThat(records).extracting(r -> mapper.readTree(r.value()).get("saleDate").asString())
				.containsExactly(DAY_1.toString(), DAY_2.toString());
		assertThat(state(DAY_1, 1)).containsEntry("status", "SENT").containsEntry("attempts", 2)
				.doesNotContainKey("lastError");

		// The status no longer shows the first error; the history does
		assertThat(history(DAY_1, 1)).containsExactly(
				"FAILED error=not acknowledged by Kafka: java.lang.RuntimeException: simulated: broker not reachable",
				"SENT offset=" + records.get(0).offset());
	}

	@Test
	void unreachableBrokerIsReportedAsPublishFailure() {
		insertDay(DAY_1, "CONFIRMED", 1);
		kafkaContainer.getDockerClient().pauseContainerCmd(kafkaContainer.getContainerId()).exec();
		try {
			assertThat(round.run()).isEqualTo(new SendRound.Result(1, 0, 1));
		}
		finally {
			kafkaContainer.getDockerClient().unpauseContainerCmd(kafkaContainer.getContainerId()).exec();
		}
		assertThat(state(DAY_1, 1)).containsEntry("status", "FAILED")
				.extractingByKey("lastError").asString().startsWith("not acknowledged by Kafka");

		assertThat(round.run()).isEqualTo(new SendRound.Result(1, 1, 0));
	}

	/** BEV 18200.00 x 410, SNK 12050.00 x 395 */
	private void insertDay(LocalDate day, String status, int revision) {
		jdbc.sql("insert into daily_sales values (?, 'BR0001', ?, ?, ?)")
				.params(day, status, revision, revision == 0 ? null : CONFIRMED_AT)
				.update();
		jdbc.sql("insert into daily_sales_line values (?, 'BEV', 18200.00, 410), (?, 'SNK', 12050.00, 395)")
				.params(day, day)
				.update();
	}

	private void confirm(LocalDate day, int revision) {
		jdbc.sql("update daily_sales set status = 'CONFIRMED', revision = ?, confirmed_at = ? where sale_date = ?")
				.params(revision, CONFIRMED_AT, day)
				.update();
	}

	/** A receipt as HQ writes it (checked against the receipt schema copy). */
	private void sendReceipt(long sourceOffset, String outcome, String rejectReason) {
		ObjectNode receipt = mapper.createObjectNode();
		receipt.put("schemaVersion", 1);
		receipt.put("branchCode", "BR0001");
		receipt.put("sourceOffset", sourceOffset);
		receipt.put("outcome", outcome);
		if (rejectReason == null) {
			receipt.put("eventId", UUID.randomUUID().toString());
			receipt.put("saleDate", DAY_1.toString());
			receipt.put("revision", 1);
			receipt.put("storedRevision", 1);
		}
		else {
			receipt.put("rejectReason", rejectReason);
			receipt.put("detail", rejectReason + ": test");
		}
		receipt.put("processedAt", OffsetDateTime.now(BANGKOK).toString());
		byte[] value = mapper.writeValueAsBytes(receipt);
		assertThat(ContractSchema.receiptErrors(value)).isEmpty();
		kafka.send(receiptTopic, "BR0001", value).join();
	}

	private static Query byId(LocalDate day, int revision) {
		return Query.query(Criteria.where("_id").is(day + "#" + revision));
	}

	private Document state(LocalDate day, int revision) {
		return mongo.findOne(byId(day, revision), Document.class, "sync_state");
	}

	/** History events in order: "FAILED error=..." for failed attempts, "RESULT offset=..." for the others. */
	private List<String> history(LocalDate day, int revision) {
		List<String> events = new ArrayList<>();
		for (Document event : state(day, revision).getList("history", Document.class)) {
			String result = event.getString("result");
			events.add(result.equals("FAILED") ? "FAILED error=" + event.getString("error")
					: result + " offset=" + event.get("offset"));
		}
		return events;
	}

	private KafkaConsumer<String, byte[]> openTopicReaderAtEnd() {
		var reader = new KafkaConsumer<>(Map.<String, Object>of(
				ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers(),
				ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false),
				new StringDeserializer(), new ByteArrayDeserializer());
		List<TopicPartition> partitions = List.of(new TopicPartition(topic, 0));
		reader.assign(partitions);
		reader.seekToEnd(partitions);
		partitions.forEach(reader::position); // resolve end offsets before the test sends anything
		return reader;
	}

	private List<ConsumerRecord<String, byte[]>> readMessages(int count) {
		List<ConsumerRecord<String, byte[]>> records = new ArrayList<>();
		long deadline = System.nanoTime() + TIMEOUT.toNanos();
		while (records.size() < count && System.nanoTime() < deadline) {
			topicReader.poll(Duration.ofMillis(500)).forEach(records::add);
		}
		assertThat(records).as("messages on %s", topic).hasSize(count);
		return records;
	}
}
