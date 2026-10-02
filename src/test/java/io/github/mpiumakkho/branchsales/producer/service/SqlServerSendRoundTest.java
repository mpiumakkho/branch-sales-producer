package io.github.mpiumakkho.branchsales.producer.service;

import org.springframework.context.annotation.Import;

import io.github.mpiumakkho.branchsales.producer.BranchDatabases;
import io.github.mpiumakkho.branchsales.producer.TestcontainersConfiguration;

/** The send round tests against a SQL Server branch database. */
@Import({ TestcontainersConfiguration.class, BranchDatabases.SqlServer.class })
class SqlServerSendRoundTest extends AbstractSendRoundTest {
}
