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
import io.github.mpiumakkho.branchsales.producer.dto.RecordType;
import io.github.mpiumakkho.branchsales.producer.service.DailyMessageWriter.Message;

/**
 * Sends one message to the topic of its type in the branch's own Kafka broker and waits for the ack ({@code acks=all}).
 */
@Component
public class RecordPublisher {

	private final KafkaTemplate<String, byte[]> template;
	private final ProducerProperties properties;
	private final Duration sendTimeout;

	public RecordPublisher(KafkaTemplate<String, byte[]> template, ProducerProperties properties) {
		this.template = template;
		this.properties = properties;
		this.sendTimeout = properties.sendTimeout();
	}

	/**
	 * Returns only after the broker acknowledged the message.
	 * @return the message's offset in the topic of its type; HQ's receipt refers to it
	 * @throws PublishException if the message was not acknowledged; it may still have been written (at-least-once)
	 */
	public long publish(RecordType type, Message message) {
		try {
			return template.send(properties.topicOf(type), message.key(), message.value())
					.get(sendTimeout.toMillis(), TimeUnit.MILLISECONDS)
					.getRecordMetadata().offset();
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
