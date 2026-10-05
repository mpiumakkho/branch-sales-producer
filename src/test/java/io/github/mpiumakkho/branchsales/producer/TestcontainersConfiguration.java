package io.github.mpiumakkho.branchsales.producer;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.mongodb.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * The branch's own Kafka broker with its two topics, and the branch's MongoDB, with the same images as the branch
 * compose file. Combine with one branch database: {@link BranchDatabases.Postgres}, {@link BranchDatabases.MySql} or
 * {@link BranchDatabases.SqlServer}.
 */
@TestConfiguration(proxyBeanMethods = false)
public class TestcontainersConfiguration {

	@Bean
	@ServiceConnection
	public KafkaContainer kafkaContainer() {
		return new KafkaContainer(DockerImageName.parse("apache/kafka:4.3.1"));
	}

	@Bean
	@ServiceConnection
	MongoDBContainer mongoDbContainer() {
		return new MongoDBContainer(DockerImageName.parse("mongo:8.0.16"));
	}

	// In a branch the topics are created by kafka-init in docker-compose.yml: one partition each
	@Bean
	NewTopic summaryTopic(@Value("${branch-sales.topic}") String name) {
		return new NewTopic(name, 1, (short) 1);
	}

	@Bean
	NewTopic receiptTopic(@Value("${branch-sales.receipt-topic}") String name) {
		return new NewTopic(name, 1, (short) 1);
	}
}
