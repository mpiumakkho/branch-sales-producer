package io.github.mpiumakkho.branchsales.producer;

import org.springframework.boot.SpringApplication;

public class TestBranchSalesProducerApplication {

	public static void main(String[] args) {
		SpringApplication.from(BranchSalesProducerApplication::main).with(TestcontainersConfiguration.class, BranchDatabases.Postgres.class).run(args);
	}

}
