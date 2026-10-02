package io.github.mpiumakkho.branchsales.producer;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Kafka with the same image as the HQ infra, and a branch PostgreSQL that
 * already has the simulated back-office tables (backoffice/postgresql/schema.sql).
 */
@TestConfiguration(proxyBeanMethods = false)
public class TestcontainersConfiguration {

	public static final int PARTITIONS = 2;

	@Bean
	@ServiceConnection
	public KafkaContainer kafkaContainer() {
		return new KafkaContainer(DockerImageName.parse("apache/kafka:4.3.1"));
	}

	@Bean
	@ServiceConnection
	PostgreSQLContainer postgresContainer() {
		return new PostgreSQLContainer(DockerImageName.parse("postgres:18.6-alpine"))
				.withInitScript("backoffice/postgresql/schema.sql");
	}

	// In the HQ infra the topic is created by kafka-init; auto topic creation is off there
	@Bean
	NewTopic summaryTopic(@Value("${branch-sales.topic}") String name) {
		return new NewTopic(name, PARTITIONS, (short) 1);
	}
}
