package io.github.mpiumakkho.branchsales.producer.kafka;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.common.KafkaException;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import io.github.mpiumakkho.branchsales.producer.config.ProducerProperties;
import io.github.mpiumakkho.branchsales.producer.service.SummaryMessageWriter.Message;

/**
 * Sends one message and waits for the broker ack ({@code acks=all}).
 */
@Component
public class SummaryPublisher {

	private final KafkaTemplate<String, byte[]> template;
	private final String topic;
	private final Duration sendTimeout;

	public SummaryPublisher(KafkaTemplate<String, byte[]> template, ProducerProperties properties) {
		this.template = template;
		this.topic = properties.topic();
		this.sendTimeout = properties.sendTimeout();
	}

	/**
	 * Returns only after the broker acknowledged the message.
	 * @throws PublishException if the message was not acknowledged; it may still have been written (at-least-once)
	 */
	public void publish(Message message) {
		try {
			template.send(topic, message.key(), message.value()).get(sendTimeout.toMillis(), TimeUnit.MILLISECONDS);
		}
		catch (ExecutionException e) {
			throw new PublishException(e.getCause());
		}
		catch (TimeoutException | KafkaException e) {
			throw new PublishException(e);
		}
		catch (org.springframework.kafka.KafkaException e) {
			// Thrown by send() itself, e.g. no topic metadata within max.block.ms when the broker is not reachable
			throw new PublishException(e);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new PublishException(e);
		}
	}

	public static class PublishException extends RuntimeException {

		public PublishException(Throwable cause) {
			// Spring wraps the client error (e.g. KafkaProducerException "Failed to send"); the innermost cause says why
			super("not acknowledged by Kafka: " + NestedExceptionUtils.getMostSpecificCause(cause), cause);
		}
	}
}
