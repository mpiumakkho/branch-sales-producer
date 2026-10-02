package io.github.mpiumakkho.branchsales.producer.service;

import org.springframework.context.annotation.Import;

import io.github.mpiumakkho.branchsales.producer.BranchDatabases;
import io.github.mpiumakkho.branchsales.producer.TestcontainersConfiguration;

/** The send round tests against a PostgreSQL branch database. */
@Import({ TestcontainersConfiguration.class, BranchDatabases.Postgres.class })
class PostgresSendRoundTest extends AbstractSendRoundTest {
}
