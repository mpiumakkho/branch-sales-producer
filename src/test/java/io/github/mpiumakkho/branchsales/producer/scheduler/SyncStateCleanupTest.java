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
import io.github.mpiumakkho.branchsales.producer.dto.RecordKey.DayKey;
import io.github.mpiumakkho.branchsales.producer.dto.RecordKey.ShiftKey;
import io.github.mpiumakkho.branchsales.producer.dto.RecordType;
import io.github.mpiumakkho.branchsales.producer.repository.SyncStateStore;

/** Q9: only accepted records (days, shift closes) older than the retention are deleted. */
@SpringBootTest(properties = {
		"branch-sales.schedule.cron=-",
		"branch-sales.schedule.cleanup-cron=-",
		"branch-sales.branch-code=BR0001",
		"branch-sales.retention=90d" })
@Import({ TestcontainersConfiguration.class, BranchDatabases.Postgres.class })
class SyncStateCleanupTest {

	private static final RecordType SUMMARY = RecordType.DAILY_SUMMARY;

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
		states.markSent(SUMMARY, new DayKey(base.plusDays(2)), 1, UUID.randomUUID(), 12);
		backdate(base.plusDays(2), Duration.ofDays(400));
		states.markSent(SUMMARY, new DayKey(base.plusDays(3)), 1, UUID.randomUUID(), 13);
		states.applyReceipt(new HqReceipt(SUMMARY, "BR0001", 13, "REJECTED", null, "UNKNOWN_CATEGORY", "test"));
		backdate(base.plusDays(3), Duration.ofDays(400));
		// A shift close accepted long ago: same rule
		states.markSent(RecordType.SHIFT_CLOSE, new ShiftKey(base, "POS01", 1), 1, UUID.randomUUID(), 20);
		states.applyReceipt(new HqReceipt(RecordType.SHIFT_CLOSE, "BR0001", 20, "INSERTED", 1, null, null));
		backdate("SHIFT_CLOSE#" + base + "#POS01#1#1", Duration.ofDays(91));

		cleanup.run();

		assertThat(mongo.findAll(org.bson.Document.class, "sync_state"))
				.extracting(d -> d.getString("_id"))
				.containsExactlyInAnyOrder("DAILY_SUMMARY#" + base.plusDays(1) + "#1", "DAILY_SUMMARY#" + base.plusDays(2) + "#1",
						"DAILY_SUMMARY#" + base.plusDays(3) + "#1");
	}

	private void accepted(LocalDate day, long offset, Duration age) {
		states.markSent(SUMMARY, new DayKey(day), 1, UUID.randomUUID(), offset);
		states.applyReceipt(new HqReceipt(SUMMARY, "BR0001", offset, "INSERTED", 1, null, null));
		backdate(day, age);
	}

	private void backdate(LocalDate day, Duration age) {
		backdate("DAILY_SUMMARY#" + day + "#1", age);
	}

	private void backdate(String id, Duration age) {
		mongo.updateFirst(Query.query(Criteria.where("_id").is(id)),
				Update.update("updatedAt", Date.from(Instant.now().minus(age))), "sync_state");
	}
}
