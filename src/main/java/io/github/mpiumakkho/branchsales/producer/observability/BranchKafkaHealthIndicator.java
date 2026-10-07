package io.github.mpiumakkho.branchsales.producer.observability;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

/**
 * Health of the branch's own Kafka broker (component {@code kafka} of /actuator/health): DOWN when the broker does
 * not answer a cluster description within a few seconds. The database and MongoDB components come from Spring Boot.
 */
@Component("kafka")
public class BranchKafkaHealthIndicator implements HealthIndicator, DisposableBean {

	// One deadline for the request and for the health call; the health thread never waits longer than this
	private static final Duration TIMEOUT = Duration.ofSeconds(5);

	private final Admin admin;

	public BranchKafkaHealthIndicator(KafkaAdmin kafkaAdmin) {
		this.admin = Admin.create(kafkaAdmin.getConfigurationProperties());
	}

	@Override
	public Health health() {
		try {
			var cluster = admin.describeCluster(new DescribeClusterOptions().timeoutMs((int) TIMEOUT.toMillis()));
			long deadline = System.nanoTime() + TIMEOUT.toNanos();
			int nodes = cluster.nodes().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS).size();
			String clusterId = cluster.clusterId().get(Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
			return Health.up().withDetail("clusterId", clusterId).withDetail("nodes", nodes).build();
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return Health.down(e).build();
		}
		catch (Exception e) {
			return Health.down(e).build();
		}
	}

	@Override
	public void destroy() {
		admin.close(TIMEOUT);
	}
}
