package io.github.mpiumakkho.branchsales.producer.send;

import java.time.Clock;
import java.time.Duration;
import java.util.random.RandomGenerator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import io.github.mpiumakkho.branchsales.producer.ProducerProperties;

/**
 * Starts a send round on the configured cron (default: every hour, Q4), after a
 * random delay of up to {@code maxJitter}, so that branches do not all read
 * their database and send at the same moment.
 */
@Component
class SendScheduler {

	private static final Logger log = LoggerFactory.getLogger(SendScheduler.class);

	private final SendRound round;
	private final TaskScheduler taskScheduler;
	private final Clock clock;
	private final Duration maxJitter;
	private final RandomGenerator random = RandomGenerator.getDefault();

	SendScheduler(SendRound round, TaskScheduler taskScheduler, Clock clock, ProducerProperties properties) {
		this.round = round;
		this.taskScheduler = taskScheduler;
		this.clock = clock;
		this.maxJitter = properties.schedule().maxJitter();
	}

	@Scheduled(cron = "${branch-sales.schedule.cron}", zone = "Asia/Bangkok")
	void startRound() {
		Duration delay = randomDelay(maxJitter, random);
		log.info("Send round starts in {}", delay);
		taskScheduler.schedule(round::run, clock.instant().plus(delay));
	}

	/** Uniform between zero and {@code max}, both included, in whole seconds. */
	static Duration randomDelay(Duration max, RandomGenerator random) {
		return Duration.ofSeconds(random.nextLong(max.toSeconds() + 1));
	}
}
