package io.github.mpiumakkho.branchsales.producer.observability;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import io.github.mpiumakkho.branchsales.producer.dto.SyncState.Status;
import io.github.mpiumakkho.branchsales.producer.repository.SyncStateStore;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * Gauges over {@code sync_state} for /actuator/prometheus. The send counters are in {@code SendRound}; the Kafka
 * client, MongoDB, data source and JVM metrics come from Spring Boot.
 * <p>
 * The counts are read from MongoDB once a minute, not during a scrape, so a scrape never waits for MongoDB; while
 * MongoDB is down the last values stay (its health component says so).
 */
@Component
public class ProducerMetrics {

	private static final Logger log = LoggerFactory.getLogger(ProducerMetrics.class);

	private final SyncStateStore states;
	private final Map<Status, AtomicLong> byStatus = new EnumMap<>(Status.class);
	private final AtomicLong resent = new AtomicLong();

	public ProducerMetrics(MeterRegistry meters, SyncStateStore states) {
		this.states = states;
		for (Status status : Status.values()) {
			AtomicLong count = new AtomicLong();
			byStatus.put(status, count);
			Gauge.builder("branch_sales.sync_state", count, AtomicLong::get)
					.description("Days (revisions) in sync_state by status, read once a minute")
					.tag("status", status.name())
					.register(meters);
		}
		Gauge.builder("branch_sales.sync_state.resent", resent, AtomicLong::get)
				.description("SENT days sent more than once and still without an HQ receipt: HQ is not reading this branch")
				.register(meters);
	}

	@Scheduled(initialDelay = 0, fixedDelay = 60_000)
	public void refresh() {
		try {
			for (Status status : Status.values()) {
				byStatus.get(status).set(states.count(status));
			}
			resent.set(states.countResent());
		}
		catch (RuntimeException e) {
			log.warn("sync_state counts not refreshed: {}", e.getMessage());
		}
	}
}
