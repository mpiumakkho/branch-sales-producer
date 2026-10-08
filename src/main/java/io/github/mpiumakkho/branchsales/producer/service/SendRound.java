package io.github.mpiumakkho.branchsales.producer.service;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import org.jspecify.annotations.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Component;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

import io.github.mpiumakkho.branchsales.producer.config.ProducerProperties;
import io.github.mpiumakkho.branchsales.producer.dto.ConfirmedDay;
import io.github.mpiumakkho.branchsales.producer.dto.RecordType;
import io.github.mpiumakkho.branchsales.producer.dto.SyncState;
import io.github.mpiumakkho.branchsales.producer.exception.InvalidSalesException;
import io.github.mpiumakkho.branchsales.producer.kafka.RecordPublisher;
import io.github.mpiumakkho.branchsales.producer.kafka.RecordPublisher.PublishException;
import io.github.mpiumakkho.branchsales.producer.repository.ConfirmedDayReader;
import io.github.mpiumakkho.branchsales.producer.repository.SyncStateStore;
import io.github.mpiumakkho.branchsales.producer.service.DailyMessageWriter.Message;

/**
 * One send round: every confirmed day (or closed shift) of every record type within the lookback whose current revision
 * still needs sending, oldest first within its type. Sales, returns, then shift closes ({@link RecordType} order):
 * HQ stores a day's returns only after its sales; shift closes do not depend on either.
 * A revision needs sending when it was never sent, its last attempt FAILED, or it was SENT but HQ has not sent a
 * receipt within {@code resend-after} (e.g. the branch broker lost the message). HQ_ACCEPTED and HQ_REJECTED are final.
 * <ul>
 * <li>SENT is written only after the branch broker's ack. If the producer stops between the ack and that write, the
 * revision is sent again next round; HQ answers DUPLICATE (rule R5).</li>
 * <li>Data that cannot become a valid message: that day is FAILED, the round continues with the next day.</li>
 * <li>Branch broker not reachable: that day is FAILED and the round stops; the rest stay pending for the next round.</li>
 * </ul>
 */
@Component
public class SendRound {

	private static final Logger log = LoggerFactory.getLogger(SendRound.class);
	private static final ZoneId BANGKOK = ZoneId.of("Asia/Bangkok");

	private final ConfirmedDayReader reader;
	private final DailyMessageWriter writer;
	private final RecordPublisher publisher;
	private final SyncStateStore states;
	private final Clock clock;
	private final Duration lookback;
	private final Duration resendAfter;
	private final AtomicBoolean running = new AtomicBoolean();
	private final Counter sentCounter;
	private final Counter failedCounter;
	private volatile @Nullable Instant lastRoundAt;

	public SendRound(ConfirmedDayReader reader, DailyMessageWriter writer, RecordPublisher publisher,
			SyncStateStore states, Clock clock, ProducerProperties properties, MeterRegistry meters) {
		this.reader = reader;
		this.writer = writer;
		this.publisher = publisher;
		this.states = states;
		this.clock = clock;
		this.lookback = properties.lookback();
		this.resendAfter = properties.resendAfter();
		this.sentCounter = Counter.builder("branch_sales.days.sent")
				.description("Days (revisions) acknowledged by the branch broker").register(meters);
		this.failedCounter = Counter.builder("branch_sales.days.failed")
				.description("Send attempts that failed: bad data or no broker ack").register(meters);
		Gauge.builder("branch_sales.send_round.last", this, r -> r.lastRoundAt == null ? 0 : r.lastRoundAt.getEpochSecond())
				.description("When the last send round finished (epoch seconds, 0 = none yet)").register(meters);
	}

	/** When the last round finished, if any. */
	public Optional<Instant> lastRound() {
		return Optional.ofNullable(lastRoundAt);
	}

	public record Result(int pending, int sent, int failed) {

		static final Result SKIPPED = new Result(0, 0, 0);
	}

	public Result run() {
		if (!running.compareAndSet(false, true)) {
			log.warn("Send round skipped: the previous round is still running");
			return Result.SKIPPED;
		}
		try {
			return sendPending();
		}
		finally {
			running.set(false);
		}
	}

	private Result sendPending() {
		List<ConfirmedDay> pending = new ArrayList<>();
		for (RecordType type : RecordType.values()) {
			try {
				pending.addAll(pending(type));
			}
			catch (DataAccessException e) {
				// e.g. a branch whose back-office has no daily_return or pos_shift tables yet, or whose login cannot read them:
				// the other types are still sent
				log.error("Cannot read confirmed {} days from the branch database, skipped this round: {}", type,
						NestedExceptionUtils.getMostSpecificCause(e).getMessage());
			}
		}
		int sent = 0;
		int failed = 0;
		for (ConfirmedDay day : pending) {
			Message message;
			try {
				message = writer.write(day);
			}
			catch (InvalidSalesException e) {
				log.warn("Not sent {} {} revision {}: {}", day.type(), day.key().text(), day.revision(), e.getMessage());
				states.markFailed(day.type(), day.key(), day.revision(), e.getMessage(), null);
				failed++;
				continue;
			}
			long offset;
			try {
				offset = publisher.publish(day.type(), message);
			}
			catch (PublishException e) {
				log.warn("Send failed for {} {} revision {}, stopping this round ({} days left pending): {}",
						day.type(), day.key().text(), day.revision(), pending.size() - sent - failed - 1, e.getMessage());
				states.markFailed(day.type(), day.key(), day.revision(), e.getMessage(), message.eventId());
				failed++;
				break;
			}
			states.markSent(day.type(), day.key(), day.revision(), message.eventId(), offset);
			log.info("Sent {} {} revision {} eventId {} offset {}", day.type(), day.key().text(), day.revision(),
					message.eventId(), offset);
			sent++;
		}
		Result result = new Result(pending.size(), sent, failed);
		sentCounter.increment(sent);
		failedCounter.increment(failed);
		lastRoundAt = clock.instant();
		log.info("Send round finished: {} pending, {} sent, {} failed", result.pending(), result.sent(), result.failed());
		return result;
	}

	private List<ConfirmedDay> pending(RecordType type) {
		LocalDate since = LocalDate.now(clock.withZone(BANGKOK)).minusDays(lookback.toDays());
		List<ConfirmedDay> confirmed = reader.readConfirmed(type, since);
		Map<String, SyncState> known = states.find(
				confirmed.stream().map(c -> SyncState.id(type, c.key(), c.revision())).toList());
		Instant resendBefore = clock.instant().minus(resendAfter);
		return confirmed.stream()
				.filter(c -> needsSending(known.get(SyncState.id(type, c.key(), c.revision())), resendBefore))
				.toList();
	}

	private static boolean needsSending(SyncState state, Instant resendBefore) {
		if (state == null) {
			return true;
		}
		return switch (state.status()) {
			case FAILED -> true;
			case SENT -> state.sentAt() == null || state.sentAt().isBefore(resendBefore);
			case HQ_ACCEPTED, HQ_REJECTED -> false;
		};
	}
}
