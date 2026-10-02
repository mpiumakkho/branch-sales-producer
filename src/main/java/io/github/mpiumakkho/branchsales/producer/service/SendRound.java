package io.github.mpiumakkho.branchsales.producer.service;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import io.github.mpiumakkho.branchsales.producer.dto.ConfirmedSales;
import io.github.mpiumakkho.branchsales.producer.exception.InvalidSalesException;
import io.github.mpiumakkho.branchsales.producer.kafka.SummaryPublisher;
import io.github.mpiumakkho.branchsales.producer.kafka.SummaryPublisher.PublishException;
import io.github.mpiumakkho.branchsales.producer.repository.PendingSalesReader;
import io.github.mpiumakkho.branchsales.producer.repository.SyncLog;
import io.github.mpiumakkho.branchsales.producer.service.SummaryMessageWriter.Message;

/**
 * One send round: every pending confirmed day, oldest first.
 * <ul>
 * <li>SENT is written only after the broker ack. If the producer stops between the ack and that write, the revision
 * is sent again next round; HQ skips it as a duplicate (rule R5).</li>
 * <li>Data that cannot become a valid message: that day is FAILED, the round continues with the next day.</li>
 * <li>Kafka not reachable: that day is FAILED and the round stops; the rest stay pending for the next round.</li>
 * </ul>
 */
@Component
public class SendRound {

	private static final Logger log = LoggerFactory.getLogger(SendRound.class);

	private final PendingSalesReader reader;
	private final SummaryMessageWriter writer;
	private final SummaryPublisher publisher;
	private final SyncLog syncLog;
	private final AtomicBoolean running = new AtomicBoolean();

	public SendRound(PendingSalesReader reader, SummaryMessageWriter writer, SummaryPublisher publisher, SyncLog syncLog) {
		this.reader = reader;
		this.writer = writer;
		this.publisher = publisher;
		this.syncLog = syncLog;
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
		List<ConfirmedSales> pending = reader.readPending();
		int sent = 0;
		int failed = 0;
		for (ConfirmedSales sales : pending) {
			Message message;
			try {
				message = writer.write(sales);
			}
			catch (InvalidSalesException e) {
				log.warn("Not sent {} revision {}: {}", sales.saleDate(), sales.revision(), e.getMessage());
				syncLog.markFailed(sales.saleDate(), sales.revision(), e.getMessage(), null);
				failed++;
				continue;
			}
			try {
				publisher.publish(message);
			}
			catch (PublishException e) {
				log.warn("Send failed for {} revision {}, stopping this round ({} days left pending): {}",
						sales.saleDate(), sales.revision(), pending.size() - sent - failed - 1, e.getMessage());
				syncLog.markFailed(sales.saleDate(), sales.revision(), e.getMessage(), message.eventId());
				failed++;
				break;
			}
			syncLog.markSent(sales.saleDate(), sales.revision(), message.eventId());
			log.info("Sent {} revision {} eventId {}", sales.saleDate(), sales.revision(), message.eventId());
			sent++;
		}
		Result result = new Result(pending.size(), sent, failed);
		log.info("Send round finished: {} pending, {} sent, {} failed", result.pending(), result.sent(), result.failed());
		return result;
	}
}
