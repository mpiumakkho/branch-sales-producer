package io.github.mpiumakkho.branchsales.producer.exception;

/**
 * The back-office data of a day cannot be written as a valid contract message
 * (e.g. a category with no HQ mapping). Sending it would only put it in the HQ
 * dead-letter topic, so it is not sent; it is logged as FAILED and tried again
 * in later rounds, after the data or the mapping is fixed.
 */
public class InvalidSalesException extends RuntimeException {

	public InvalidSalesException(String message) {
		super(message);
	}
}
