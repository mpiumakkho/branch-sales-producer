package io.github.mpiumakkho.branchsales.producer.repository;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;

import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;

import io.github.mpiumakkho.branchsales.producer.BranchDatabases;
import io.github.mpiumakkho.branchsales.producer.TestcontainersConfiguration;
import io.github.mpiumakkho.branchsales.producer.dto.RecordKey.DayKey;
import io.github.mpiumakkho.branchsales.producer.dto.RecordKey.ShiftKey;
import io.github.mpiumakkho.branchsales.producer.dto.RecordType;
import io.github.mpiumakkho.branchsales.producer.dto.SyncState;

/** Ids and key fields of sync_state documents for both key kinds (requirements §6). */
@SpringBootTest(properties = {
		"branch-sales.schedule.cron=-",
		"branch-sales.schedule.cleanup-cron=-",
		"branch-sales.branch-code=BR0001" })
@Import({ TestcontainersConfiguration.class, BranchDatabases.Postgres.class })
class SyncStateStoreTest {

	private static final LocalDate DAY = LocalDate.of(2026, 10, 1);
	private static final ShiftKey SHIFT = new ShiftKey(DAY, "POS01", 1);

	@Autowired
	SyncStateStore states;

	@Autowired
	MongoTemplate mongo;

	@BeforeEach
	void setUp() {
		mongo.remove(new Query(), SyncStateStore.COLLECTION);
	}

	@Test
	void shiftIdHasFiveSegmentsAndStoresTerminalAndShift() {
		states.markSent(RecordType.SHIFT_CLOSE, SHIFT, 2, UUID.randomUUID(), 5);
		states.markSent(RecordType.DAILY_SUMMARY, new DayKey(DAY), 1, UUID.randomUUID(), 5);

		Document shift = mongo.findById("SHIFT_CLOSE#2026-10-01#POS01#1#2", Document.class, SyncStateStore.COLLECTION);
		assertThat(shift).isNotNull()
				.containsEntry("type", "SHIFT_CLOSE")
				.containsEntry("date", "2026-10-01")
				.containsEntry("terminalId", "POS01")
				.containsEntry("shiftNo", 1)
				.containsEntry("revision", 2)
				.containsEntry("status", "SENT");
		// The daily types keep their id and fields
		Document day = mongo.findById("DAILY_SUMMARY#2026-10-01#1", Document.class, SyncStateStore.COLLECTION);
		assertThat(day).isNotNull().containsEntry("date", "2026-10-01").doesNotContainKeys("terminalId", "shiftNo");
	}

	@Test
	void findRebuildsAShiftKey() {
		states.markFailed(RecordType.SHIFT_CLOSE, SHIFT, 1, "no HQ tender mapping for local tender [GV]", null);
		states.markSent(RecordType.DAILY_RETURN, new DayKey(DAY), 1, UUID.randomUUID(), 3);

		String shiftId = SyncState.id(RecordType.SHIFT_CLOSE, SHIFT, 1);
		String returnId = SyncState.id(RecordType.DAILY_RETURN, new DayKey(DAY), 1);
		var found = states.find(List.of(shiftId, returnId));

		assertThat(found.get(shiftId)).isNotNull().satisfies(s -> {
			assertThat(s.type()).isEqualTo(RecordType.SHIFT_CLOSE);
			assertThat(s.key()).isEqualTo(SHIFT);
			assertThat(s.status()).isEqualTo(SyncState.Status.FAILED);
		});
		assertThat(found.get(returnId).key()).isEqualTo(new DayKey(DAY));
	}

	@Test
	void legacyIdMigrationIgnoresShiftIds() {
		states.markSent(RecordType.SHIFT_CLOSE, SHIFT, 1, UUID.randomUUID(), 7);
		// A document from before the return topic existed: id date#revision, no type
		mongo.insert(new Document("_id", "2026-09-30#1").append("saleDate", "2026-09-30").append("revision", 1)
				.append("status", "HQ_ACCEPTED").append("attempts", 1), SyncStateStore.COLLECTION);

		new SyncStateStore(mongo, Clock.systemUTC()); // what start-up does

		assertThat(mongo.findAll(Document.class, SyncStateStore.COLLECTION)).extracting(d -> d.getString("_id"))
				.containsExactlyInAnyOrder("SHIFT_CLOSE#2026-10-01#POS01#1#1", "DAILY_SUMMARY#2026-09-30#1");
		assertThat(mongo.findById("SHIFT_CLOSE#2026-10-01#POS01#1#1", Document.class, SyncStateStore.COLLECTION))
				.containsEntry("type", "SHIFT_CLOSE").containsEntry("terminalId", "POS01");
	}
}
