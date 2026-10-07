package io.github.mpiumakkho.branchsales.producer.observability;

import java.time.Clock;
import java.time.Duration;

import org.springframework.stereotype.Component;

import io.github.mpiumakkho.branchsales.producer.config.ProducerProperties;
import io.github.mpiumakkho.branchsales.producer.dto.SyncState.Status;
import io.github.mpiumakkho.branchsales.producer.repository.SyncStateStore;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * Gauges over {@code sync_state} for /actuator/prometheus, one count query each per scrape. The send counters are in
 * {@code SendRound}; the Kafka client, MongoDB, data source and JVM metrics come from Spring Boot.
 */
@Component
public class ProducerMetrics {

	public ProducerMetrics(MeterRegistry meters, SyncStateStore states, Clock clock, ProducerProperties properties) {
		for (Status status : Status.values()) {
			Gauge.builder("branch_sales.sync_state", states, s -> s.count(status))
					.description("Days (revisions) in sync_state by status")
					.tag("status", status.name())
					.register(meters);
		}
		Duration resendAfter = properties.resendAfter();
		Gauge.builder("branch_sales.sync_state.overdue", states,
				s -> s.countSentBefore(clock.instant().minus(resendAfter)))
				.description("SENT days without an HQ receipt for longer than resend-after: HQ is not reading this branch")
				.register(meters);
	}
}
