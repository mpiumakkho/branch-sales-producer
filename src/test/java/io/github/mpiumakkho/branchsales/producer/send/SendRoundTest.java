package io.github.mpiumakkho.branchsales.producer.send;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.testcontainers.kafka.KafkaContainer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import io.github.mpiumakkho.branchsales.producer.ContractSchema;
import io.github.mpiumakkho.branchsales.producer.TestcontainersConfiguration;

/**
 * Runs send rounds against a branch PostgreSQL with the simulated back-office
 * tables and a real Kafka broker. Rounds are started directly; the cron is off.
 */
@SpringBootTest(properties = {
		"branch-sales.schedule.cron=-",
		"branch-sales.category-mapping.BEV=BEVERAGE",
		"branch-sales.category-mapping.SNK=SNACK",
		"spring.kafka.producer.properties.delivery.timeout.ms=15000",
		"spring.kafka.producer.properties.request.timeout.ms=5000" })
@Import(TestcontainersConfiguration.class)
class SendRoundTest {

	private static final LocalDate DAY_1 = LocalDate.of(2026, 10, 1);
	private static final LocalDate DAY_2 = LocalDate.of(2026, 10, 2);
	private static final OffsetDateTime CONFIRMED_AT = OffsetDateTime.parse("2026-10-01T21:45:00+07:00");

	@Autowired
	SendRound round;

	@MockitoSpyBean
	SummaryPublisher publisher;

	@Autowired
	JdbcClient jdbc;

	// Not ${spring.kafka.bootstrap-servers}: @ServiceConnection does not set that property, so it would resolve to
	// the application.yaml default (a local HQ Kafka, if one is running) instead of the test container
	@Autowired
	KafkaContainer kafkaContainer;

	@Value("${branch-sales.topic}")
	String topic;

	private final JsonMapper mapper = JsonMapper.builder().build();
	private KafkaConsumer<String, byte[]> topicReader;

	@BeforeEach
	void setUp() {
		jdbc.sql("delete from sync_log").update();
		jdbc.sql("delete from daily_sales_line").update();
		jdbc.sql("delete from daily_sales").update();
		topicReader = openTopicReaderAtEnd();
	}

	@AfterEach
	void tearDown() {
		topicReader.close();
	}

	@Test
	void sendsConfirmedDayOnceAndLogsSent() {
		insertDay(DAY_1, "CONFIRMED", 1);

		assertThat(round.run()).isEqualTo(new SendRound.Result(1, 1, 0));

		ConsumerRecord<String, byte[]> record = readMessages(1).getFirst();
		assertThat(record.key()).isEqualTo("BR0001");
		assertThat(ContractSchema.errors(record.value())).isEmpty();
		JsonNode json = mapper.readTree(record.value());
		assertThat(json.get("saleDate").asString()).isEqualTo("2026-10-01");
		assertThat(json.get("revision").intValue()).isEqualTo(1);
		assertThat(json.get("confirmedAt").asString()).isEqualTo("2026-10-01T21:45:00+07:00");
		assertThat(json.get("totalAmount").asString()).isEqualTo("30250.00");

		assertThat(syncLog(DAY_1, 1)).containsEntry("status", "SENT").containsEntry("attempts", 1)
				.containsEntry("event_id", json.get("eventId").asString())
				.containsEntry("last_error", null);
		assertThat(syncLog(DAY_1, 1).get("sent_at")).isNotNull();

		// Nothing pending any more: the next round sends nothing
		assertThat(round.run()).isEqualTo(new SendRound.Result(0, 0, 0));
		assertThat(topicReader.poll(Duration.ofSeconds(1))).isEmpty();
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
		assertThat(syncLog(DAY_1, 1)).containsEntry("status", "SENT");
		assertThat(syncLog(DAY_1, 2)).containsEntry("status", "SENT");
	}

	@Test
	void dayWithUnmappedCategoryIsLoggedFailedAndOtherDaysAreStillSent() {
		insertDay(DAY_1, "CONFIRMED", 1);
		jdbc.sql("insert into daily_sales_line values (?, 'LOTTO', 500.00, 5)").param(DAY_1).update();
		insertDay(DAY_2, "CONFIRMED", 1);

		assertThat(round.run()).isEqualTo(new SendRound.Result(2, 1, 1));

		assertThat(mapper.readTree(readMessages(1).getFirst().value()).get("saleDate").asString())
				.isEqualTo("2026-10-02");
		assertThat(syncLog(DAY_1, 1)).containsEntry("status", "FAILED").containsEntry("attempts", 1)
				.containsEntry("last_error", "no HQ category mapping for local category [LOTTO]");
		assertThat(syncLog(DAY_2, 1)).containsEntry("status", "SENT");

		// Still pending: tried again every round until the data or the mapping is fixed
		assertThat(round.run()).isEqualTo(new SendRound.Result(1, 0, 1));
		assertThat(syncLog(DAY_1, 1)).containsEntry("attempts", 2);
	}

	@Test
	void kafkaFailureStopsTheRoundAndTheNextRoundSendsEverything() {
		insertDay(DAY_1, "CONFIRMED", 1);
		insertDay(DAY_2, "CONFIRMED", 1);
		doThrow(new SummaryPublisher.PublishException(new RuntimeException("simulated: broker not reachable")))
				.doCallRealMethod()
				.when(publisher).publish(any());

		assertThat(round.run()).isEqualTo(new SendRound.Result(2, 0, 1));
		assertThat(syncLog(DAY_1, 1)).containsEntry("status", "FAILED")
				.extractingByKey("last_error").asString().contains("simulated: broker not reachable");
		assertThat(syncLog(DAY_2, 1)).isEmpty(); // not attempted
		assertThat(topicReader.poll(Duration.ofSeconds(1))).isEmpty();

		assertThat(round.run()).isEqualTo(new SendRound.Result(2, 2, 0));
		List<ConsumerRecord<String, byte[]>> records = readMessages(2);
		// Oldest day first, same key, so HQ reads them in order
		assertThat(records).extracting(r -> mapper.readTree(r.value()).get("saleDate").asString())
				.containsExactly("2026-10-01", "2026-10-02");
		assertThat(syncLog(DAY_1, 1)).containsEntry("status", "SENT").containsEntry("attempts", 2)
				.containsEntry("last_error", null);
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
		assertThat(syncLog(DAY_1, 1)).containsEntry("status", "FAILED")
				.extractingByKey("last_error").asString().startsWith("not acknowledged by Kafka");

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

	private Map<String, Object> syncLog(LocalDate day, int revision) {
		return jdbc.sql("select status, attempts, last_error, event_id, sent_at from sync_log where sale_date = ? and revision = ?")
				.params(day, revision)
				.query()
				.listOfRows()
				.stream().findFirst().orElse(Map.of());
	}

	private KafkaConsumer<String, byte[]> openTopicReaderAtEnd() {
		var reader = new KafkaConsumer<>(Map.<String, Object>of(
				ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers(),
				ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false),
				new StringDeserializer(), new ByteArrayDeserializer());
		List<TopicPartition> partitions = IntStream.range(0, TestcontainersConfiguration.PARTITIONS)
				.mapToObj(p -> new TopicPartition(topic, p))
				.toList();
		reader.assign(partitions);
		reader.seekToEnd(partitions);
		partitions.forEach(reader::position); // resolve end offsets before the test sends anything
		return reader;
	}

	private List<ConsumerRecord<String, byte[]>> readMessages(int count) {
		List<ConsumerRecord<String, byte[]>> records = new ArrayList<>();
		long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
		while (records.size() < count && System.nanoTime() < deadline) {
			topicReader.poll(Duration.ofMillis(500)).forEach(records::add);
		}
		assertThat(records).as("messages on %s", topic).hasSize(count);
		return records;
	}
}
