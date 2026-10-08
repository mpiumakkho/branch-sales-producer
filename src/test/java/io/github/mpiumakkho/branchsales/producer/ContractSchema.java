package io.github.mpiumakkho.branchsales.producer;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;

import com.networknt.schema.Error;
import com.networknt.schema.InputFormat;
import com.networknt.schema.Schema;
import com.networknt.schema.SchemaRegistry;
import com.networknt.schema.SchemaRegistryConfig;
import com.networknt.schema.SpecificationVersion;

/** The contract schema copies in contract/, with format checks on (as the HQ consumer validates). */
public final class ContractSchema {

	private static final Schema SUMMARY = load("/contract/daily-sales-summary.v1.schema.json");
	private static final Schema RETURN = load("/contract/daily-return.v1.schema.json");
	private static final Schema SHIFT = load("/contract/shift-close.v1.schema.json");
	private static final Schema RECEIPT = load("/contract/daily-sales-receipt.v1.schema.json");

	private ContractSchema() {
	}

	/** @return schema errors of a summary message, empty if it is valid */
	public static List<String> errors(byte[] message) {
		return errors(SUMMARY, message);
	}

	/** @return schema errors of a return message, empty if it is valid */
	public static List<String> returnErrors(byte[] message) {
		return errors(RETURN, message);
	}

	/** @return schema errors of a shift close message, empty if it is valid */
	public static List<String> shiftErrors(byte[] message) {
		return errors(SHIFT, message);
	}

	/** @return schema errors of a receipt, empty if it is valid */
	public static List<String> receiptErrors(byte[] receipt) {
		return errors(RECEIPT, receipt);
	}

	private static List<String> errors(Schema schema, byte[] json) {
		return schema.validate(new String(json, StandardCharsets.UTF_8), InputFormat.JSON)
				.stream().map(Error::toString).toList();
	}

	private static Schema load(String resource) {
		var config = SchemaRegistryConfig.builder().formatAssertionsEnabled(true).locale(Locale.ENGLISH).build();
		var registry = SchemaRegistry.withDefaultDialect(SpecificationVersion.DRAFT_2020_12,
				builder -> builder.schemaRegistryConfig(config));
		try (InputStream in = ContractSchema.class.getResourceAsStream(resource)) {
			return registry.getSchema(in);
		}
		catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}
}
