package io.github.mpiumakkho.branchsales.producer.send;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.random.RandomGenerator;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

class SendSchedulerTest {

	@Test
	void randomDelayStaysWithinMaxJitter() {
		Duration max = Duration.ofMinutes(30);
		RandomGenerator random = RandomGenerator.of("L64X128MixRandom");

		var delays = IntStream.range(0, 10_000).mapToObj(i -> SendScheduler.randomDelay(max, random)).toList();

		assertThat(delays).allMatch(d -> !d.isNegative() && d.compareTo(max) <= 0);
		// Spread over the window, not clustered at one end
		assertThat(delays).anyMatch(d -> d.compareTo(Duration.ofMinutes(3)) < 0);
		assertThat(delays).anyMatch(d -> d.compareTo(Duration.ofMinutes(27)) > 0);
	}

	@Test
	void zeroJitterMeansNoDelay() {
		assertThat(SendScheduler.randomDelay(Duration.ZERO, RandomGenerator.getDefault())).isZero();
	}
}
