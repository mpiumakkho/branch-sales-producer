package io.github.mpiumakkho.branchsales.producer.scheduler;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import io.github.mpiumakkho.branchsales.producer.config.ProducerProperties;
import io.github.mpiumakkho.branchsales.producer.repository.SyncStateStore;

/**
 * Deletes the send state of days HQ accepted once it is older than {@code retention} (Q9: 90 days by default), so
 * the branch's MongoDB does not grow forever. Runs once a day; days still waiting or rejected are never deleted.
 */
@Component
public class SyncStateCleanup {

	private static final Logger log = LoggerFactory.getLogger(SyncStateCleanup.class);

	private final SyncStateStore states;
	private final Clock clock;
	private final Duration retention;

	public SyncStateCleanup(SyncStateStore states, Clock clock, ProducerProperties properties) {
		this.states = states;
		this.clock = clock;
		this.retention = properties.retention();
	}

	@Scheduled(cron = "${branch-sales.schedule.cleanup-cron}", zone = "Asia/Bangkok")
	public void run() {
		Instant cutoff = clock.instant().minus(retention);
		long deleted = states.deleteAcceptedBefore(cutoff);
		log.info("Send state clean-up: {} accepted days older than {} deleted", deleted, retention);
	}
}
