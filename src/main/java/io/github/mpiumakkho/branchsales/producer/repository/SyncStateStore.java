package io.github.mpiumakkho.branchsales.producer.repository;

import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.bson.Document;
import org.jspecify.annotations.Nullable;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;

import io.github.mpiumakkho.branchsales.producer.dto.HqReceipt;
import io.github.mpiumakkho.branchsales.producer.dto.SyncState;
import io.github.mpiumakkho.branchsales.producer.dto.SyncState.Status;

/**
 * Send state in the branch's MongoDB, collection {@code sync_state}: one document per (saleDate, revision) with the
 * current status and the last {@value #HISTORY_SIZE} events (send attempts and HQ receipts).
 * <p>
 * Every change is one update of one document, which MongoDB applies atomically, so status and history always agree
 * without a transaction (no replica set needed). Each change is written as soon as it happens; nothing is buffered.
 * <p>
 * HQ_ACCEPTED is final: a later send attempt or a REJECTED receipt for another copy of the message is recorded in the
 * history but does not change the status.
 */
@Repository
public class SyncStateStore {

	static final String COLLECTION = "sync_state";
	static final int HISTORY_SIZE = 50;
	static final int MAX_ERROR_LENGTH = 1000;

	private static final List<String> FINAL = List.of(Status.HQ_ACCEPTED.name(), Status.HQ_REJECTED.name());

	private final MongoTemplate mongo;
	private final Clock clock;

	public SyncStateStore(MongoTemplate mongo, Clock clock) {
		this.mongo = mongo;
		this.clock = clock;
		// Receipts are matched by the offsets the revision was sent at
		mongo.indexOps(COLLECTION).createIndex(new Index("sentOffsets", Sort.Direction.ASC));
		mongo.indexOps(COLLECTION).createIndex(new Index("status", Sort.Direction.ASC));
		// Clean-up of accepted days by their last change
		mongo.indexOps(COLLECTION).createIndex(new Index("updatedAt", Sort.Direction.ASC));
	}

	/** Documents in the given status (metrics). */
	public long count(Status status) {
		return mongo.count(query(where("status").is(status.name())), COLLECTION);
	}

	/**
	 * SENT documents sent more than once: the first copy got no HQ receipt within resend-after, and the resend has
	 * none yet either (metrics). Stays counted until a receipt arrives, unlike the age of the last send.
	 */
	public long countResent() {
		return mongo.count(query(where("status").is(Status.SENT.name()).and("attempts").gt(1)), COLLECTION);
	}

	/** @return states by {@link SyncState#id}; days never sent have no entry */
	public Map<String, SyncState> find(Collection<String> ids) {
		Map<String, SyncState> states = new HashMap<>();
		for (Document doc : mongo.find(query(where("_id").in(ids)), Document.class, COLLECTION)) {
			Date sentAt = doc.getDate("sentAt");
			states.put(doc.getString("_id"), new SyncState(
					LocalDate.parse(doc.getString("saleDate")),
					doc.getInteger("revision"),
					Status.valueOf(doc.getString("status")),
					doc.getInteger("attempts", 0),
					sentAt == null ? null : sentAt.toInstant()));
		}
		return states;
	}

	/** Called only after the branch broker acknowledged the message. */
	public void markSent(LocalDate saleDate, int revision, UUID eventId, long offset) {
		Instant now = clock.instant();
		Document event = event(now, "SENT").append("eventId", eventId.toString()).append("offset", offset);
		Update update = new Update()
				.set("status", Status.SENT.name())
				.unset("lastError")
				.set("eventId", eventId.toString())
				.set("sentAt", now)
				.set("updatedAt", now)
				.inc("attempts", 1)
				.addToSet("sentOffsets", offset);
		update.push("history").slice(-HISTORY_SIZE).each(event);
		writeUnlessFinal(saleDate, revision, update, event, offset);
	}

	/**
	 * The revision stays pending and is tried again in the next round.
	 * @param eventId the message that was not acknowledged, or null if no message could be written from the data.
	 *                A message that was not acknowledged may still have reached Kafka, so HQ may still receive it.
	 */
	public void markFailed(LocalDate saleDate, int revision, String error, @Nullable UUID eventId) {
		Instant now = clock.instant();
		String lastError = error.length() > MAX_ERROR_LENGTH ? error.substring(0, MAX_ERROR_LENGTH) : error;
		Document event = event(now, "FAILED").append("error", lastError)
				.append("eventId", eventId == null ? null : eventId.toString());
		Update update = new Update()
				.set("status", Status.FAILED.name())
				.set("lastError", lastError)
				.set("updatedAt", now)
				.inc("attempts", 1);
		update.push("history").slice(-HISTORY_SIZE).each(event);
		writeUnlessFinal(saleDate, revision, update, event, null);
	}

	/**
	 * Deletes the state of days HQ accepted whose last change is before {@code cutoff} (Q9). Days still waiting
	 * (SENT, FAILED) or rejected by HQ are kept, whatever their age, so they stay visible.
	 * @return number of documents deleted
	 */
	public long deleteAcceptedBefore(Instant cutoff) {
		return mongo.remove(query(where("status").is(Status.HQ_ACCEPTED.name()).and("updatedAt").lt(Date.from(cutoff))),
				COLLECTION).getDeletedCount();
	}

	/**
	 * Applies an HQ receipt to the revision that was sent at its source offset.
	 * @return false if no revision was sent at that offset (yet)
	 */
	public boolean applyReceipt(HqReceipt receipt) {
		Instant now = clock.instant();
		Document event = event(now, "HQ_" + receipt.outcome()).append("offset", receipt.sourceOffset());
		Update update = new Update().set("updatedAt", now).set("hqReceivedAt", now).set("hqOutcome", receipt.outcome());
		if (receipt.accepted()) {
			event.append("storedRevision", receipt.storedRevision());
			update.set("status", Status.HQ_ACCEPTED.name())
					.set("hqStoredRevision", receipt.storedRevision())
					.unset("hqRejectReason")
					.unset("hqDetail");
		}
		else {
			event.append("rejectReason", receipt.rejectReason()).append("error", receipt.detail());
			update.set("status", Status.HQ_REJECTED.name())
					.set("hqRejectReason", receipt.rejectReason())
					.set("hqDetail", receipt.detail());
		}
		update.push("history").slice(-HISTORY_SIZE).each(event);

		Query sentAtOffset = query(where("sentOffsets").is(receipt.sourceOffset()));
		if (receipt.accepted()) {
			return mongo.updateFirst(sentAtOffset, update, COLLECTION).getMatchedCount() > 0;
		}
		Query notAccepted = query(where("sentOffsets").is(receipt.sourceOffset()).and("status").ne(Status.HQ_ACCEPTED.name()));
		if (mongo.updateFirst(notAccepted, update, COLLECTION).getMatchedCount() > 0) {
			return true;
		}
		// Already accepted through another copy: keep the status, record the receipt
		return mongo.updateFirst(sentAtOffset, historyOnly(event, null), COLLECTION).getMatchedCount() > 0;
	}

	/**
	 * Upserts the document unless it is final. A final document (or a concurrent insert) makes the upsert fail on the
	 * duplicate _id; then only the event is recorded.
	 */
	private void writeUnlessFinal(LocalDate saleDate, int revision, Update update, Document event, @Nullable Long offset) {
		String id = SyncState.id(saleDate, revision);
		update.setOnInsert("saleDate", saleDate.toString()).setOnInsert("revision", revision);
		try {
			mongo.upsert(query(where("_id").is(id).and("status").nin(FINAL)), update, COLLECTION);
		}
		catch (DuplicateKeyException e) {
			mongo.updateFirst(query(where("_id").is(id)), historyOnly(event, offset), COLLECTION);
		}
	}

	private static Update historyOnly(Document event, @Nullable Long offset) {
		Update update = new Update();
		if (offset != null) {
			update.addToSet("sentOffsets", offset);
		}
		update.push("history").slice(-HISTORY_SIZE).each(event);
		return update;
	}

	private static Document event(Instant at, String result) {
		return new Document("at", Date.from(at)).append("result", result);
	}
}
