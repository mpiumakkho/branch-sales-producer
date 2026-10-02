package io.github.mpiumakkho.branchsales.producer;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.mssqlserver.MSSQLServerContainer;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * One branch database per supported back-office database, each started with its simulated back-office tables
 * (backoffice/&lt;vendor&gt;/schema.sql). The producer's own tables come from its Flyway migrations, as in a branch.
 */
public final class BranchDatabases {

	private BranchDatabases() {
	}

	@TestConfiguration(proxyBeanMethods = false)
	public static class Postgres {

		@Bean
		@ServiceConnection
		PostgreSQLContainer postgresContainer() {
			return new PostgreSQLContainer(DockerImageName.parse("postgres:18.6-alpine"))
					.withInitScript("backoffice/postgresql/schema.sql");
		}
	}

	@TestConfiguration(proxyBeanMethods = false)
	public static class MySql {

		@Bean
		@ServiceConnection
		MySQLContainer mySqlContainer() {
			return new MySQLContainer(DockerImageName.parse("mysql:8.4.11"))
					.withInitScript("backoffice/mysql/schema.sql")
					// The back-office stores local Bangkok time without a zone (DATETIME); read and write it as such
					.withUrlParam("connectionTimeZone", "Asia/Bangkok");
		}
	}

	@TestConfiguration(proxyBeanMethods = false)
	public static class SqlServer {

		@Bean
		@ServiceConnection
		MSSQLServerContainer sqlServerContainer() {
			return new MSSQLServerContainer(DockerImageName.parse("mcr.microsoft.com/mssql/server:2022-CU27-ubuntu-22.04"))
					.acceptLicense()
					.withInitScript("backoffice/sqlserver/schema.sql");
		}
	}
}
