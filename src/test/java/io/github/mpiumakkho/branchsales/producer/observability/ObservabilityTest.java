package io.github.mpiumakkho.branchsales.producer.observability;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.Environment;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.web.client.RestClient;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import io.github.mpiumakkho.branchsales.producer.BranchDatabases;
import io.github.mpiumakkho.branchsales.producer.TestcontainersConfiguration;
import io.github.mpiumakkho.branchsales.producer.dto.HqReceipt;
import io.github.mpiumakkho.branchsales.producer.repository.SyncStateStore;
import io.github.mpiumakkho.branchsales.producer.service.SendRound;

/**
 * The health endpoint reports the branch's Kafka, MongoDB and database, and the Prometheus endpoint carries the
 * sync_state gauges and the send counters. Served on a random port here; PRODUCER_HTTP_PORT in production.
 */
@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT, properties = {
		"branch-sales.schedule.cron=-",
		"branch-sales.schedule.cleanup-cron=-",
		"branch-sales.branch-code=BR0001",
		"branch-sales.resend-after=24h" })
@Import({ TestcontainersConfiguration.class, BranchDatabases.Postgres.class })
class ObservabilityTest {

	private static final JsonMapper MAPPER = JsonMapper.builder().build();

	@Autowired
	SyncStateStore states;

	@Autowired
	MongoTemplate mongo;

	@Autowired
	SendRound round;

	@Autowired
	Environment environment;

	private RestClient http;

	@BeforeEach
	void setUp() {
		mongo.remove(new Query(), "sync_state");
		http = RestClient.create("http://localhost:" + environment.getRequiredProperty("local.server.port"));
	}

	@Test
	void healthReportsKafkaMongoAndDatabase() {
		JsonNode health = MAPPER.readTree(http.get().uri("/actuator/health").retrieve().body(String.class));
		assertThat(health.path("status").asString()).isEqualTo("UP");
		for (String component : List.of("kafka", "mongo", "db")) {
			assertThat(health.path("components").path(component).path("status").asString()).as(component).isEqualTo("UP");
		}
		assertThat(health.path("components").path("kafka").path("details").path("nodes").asInt()).isEqualTo(1);
	}

	@Test
	void prometheusReportsSyncStateAndRounds() {
		LocalDate base = LocalDate.of(2026, 1, 1);
		states.markSent(base, 1, UUID.randomUUID(), 10);                 // SENT, recent
		states.markSent(base.plusDays(1), 1, UUID.randomUUID(), 11);     // SENT for 2 days: overdue
		mongo.updateFirst(Query.query(Criteria.where("_id").is(base.plusDays(1) + "#1")),
				Update.update("sentAt", Date.from(Instant.now().minus(Duration.ofDays(2)))), "sync_state");
		states.markSent(base.plusDays(2), 1, UUID.randomUUID(), 12);
		states.applyReceipt(new HqReceipt("BR0001", 12, "INSERTED", 1, null, null));   // HQ_ACCEPTED
		states.markFailed(base.plusDays(3), 1, "no mapping", null);                     // FAILED
		round.run(); // nothing confirmed in the empty branch database: 0 sent, 0 failed

		List<String> lines = http.get().uri("/actuator/prometheus").retrieve().body(String.class).lines().toList();
		assertThat(lines).contains(
				"branch_sales_sync_state{status=\"SENT\"} 2.0",
				"branch_sales_sync_state{status=\"FAILED\"} 1.0",
				"branch_sales_sync_state{status=\"HQ_ACCEPTED\"} 1.0",
				"branch_sales_sync_state{status=\"HQ_REJECTED\"} 0.0",
				"branch_sales_sync_state_overdue 1.0",
				"branch_sales_days_sent_total 0.0",
				"branch_sales_days_failed_total 0.0");
		assertThat(lines).anyMatch(line -> line.startsWith("branch_sales_send_round_last ") && !line.endsWith(" 0.0"));
	}
}
