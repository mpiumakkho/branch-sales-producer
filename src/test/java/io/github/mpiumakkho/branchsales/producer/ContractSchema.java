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

/** The contract schema copy in contract/, with format checks on (as the HQ consumer validates). */
public final class ContractSchema {

	private static final Schema SCHEMA = load();

	private ContractSchema() {
	}

	/** @return schema errors, empty if the message is valid */
	public static List<String> errors(byte[] message) {
		return SCHEMA.validate(new String(message, StandardCharsets.UTF_8), InputFormat.JSON)
				.stream().map(Error::toString).toList();
	}

	private static Schema load() {
		var config = SchemaRegistryConfig.builder().formatAssertionsEnabled(true).locale(Locale.ENGLISH).build();
		var registry = SchemaRegistry.withDefaultDialect(SpecificationVersion.DRAFT_2020_12,
				builder -> builder.schemaRegistryConfig(config));
		try (InputStream in = ContractSchema.class.getResourceAsStream("/contract/daily-sales-summary.v1.schema.json")) {
			return registry.getSchema(in);
		}
		catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}
}
