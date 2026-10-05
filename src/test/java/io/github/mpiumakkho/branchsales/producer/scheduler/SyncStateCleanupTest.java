package io.github.mpiumakkho.branchsales.producer.scheduler;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Date;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import io.github.mpiumakkho.branchsales.producer.BranchDatabases;
import io.github.mpiumakkho.branchsales.producer.TestcontainersConfiguration;
import io.github.mpiumakkho.branchsales.producer.dto.HqReceipt;
import io.github.mpiumakkho.branchsales.producer.repository.SyncStateStore;

/** Q9: only accepted days older than the retention are deleted. */
@SpringBootTest(properties = {
		"branch-sales.schedule.cron=-",
		"branch-sales.schedule.cleanup-cron=-",
		"branch-sales.branch-code=BR0001",
		"branch-sales.retention=90d" })
@Import({ TestcontainersConfiguration.class, BranchDatabases.Postgres.class })
class SyncStateCleanupTest {

	@Autowired
	SyncStateCleanup cleanup;

	@Autowired
	SyncStateStore states;

	@Autowired
	MongoTemplate mongo;

	@Test
	void deletesOnlyAcceptedDaysOlderThanRetention() {
		mongo.remove(new Query(), "sync_state");
		LocalDate base = LocalDate.of(2026, 1, 1);
		// Accepted long ago, accepted recently, still waiting for HQ (old), rejected by HQ (old)
		accepted(base, 10, Duration.ofDays(91));
		accepted(base.plusDays(1), 11, Duration.ofDays(89));
		states.markSent(base.plusDays(2), 1, UUID.randomUUID(), 12);
		backdate(base.plusDays(2), Duration.ofDays(400));
		states.markSent(base.plusDays(3), 1, UUID.randomUUID(), 13);
		states.applyReceipt(new HqReceipt("BR0001", 13, "REJECTED", null, "UNKNOWN_CATEGORY", "test"));
		backdate(base.plusDays(3), Duration.ofDays(400));

		cleanup.run();

		assertThat(mongo.findAll(org.bson.Document.class, "sync_state"))
				.extracting(d -> d.getString("_id"))
				.containsExactlyInAnyOrder(base.plusDays(1) + "#1", base.plusDays(2) + "#1", base.plusDays(3) + "#1");
	}

	private void accepted(LocalDate day, long offset, Duration age) {
		states.markSent(day, 1, UUID.randomUUID(), offset);
		states.applyReceipt(new HqReceipt("BR0001", offset, "INSERTED", 1, null, null));
		backdate(day, age);
	}

	private void backdate(LocalDate day, Duration age) {
		mongo.updateFirst(Query.query(Criteria.where("_id").is(day + "#1")),
				Update.update("updatedAt", Date.from(Instant.now().minus(age))), "sync_state");
	}
}
