package io.github.mpiumakkho.branchsales.producer.observability;

import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.Admin;
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

	private static final long TIMEOUT_SECONDS = 5;

	private final Admin admin;

	public BranchKafkaHealthIndicator(KafkaAdmin kafkaAdmin) {
		this.admin = Admin.create(kafkaAdmin.getConfigurationProperties());
	}

	@Override
	public Health health() {
		try {
			var cluster = admin.describeCluster();
			int nodes = cluster.nodes().get(TIMEOUT_SECONDS, TimeUnit.SECONDS).size();
			return Health.up().withDetail("clusterId", cluster.clusterId().get(TIMEOUT_SECONDS, TimeUnit.SECONDS))
					.withDetail("nodes", nodes).build();
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
		admin.close();
	}
}
