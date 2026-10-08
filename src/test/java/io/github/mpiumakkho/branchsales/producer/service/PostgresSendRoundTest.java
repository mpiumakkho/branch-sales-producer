package io.github.mpiumakkho.branchsales.producer.service;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDate;
import java.time.ZoneId;

import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.Import;

import io.github.mpiumakkho.branchsales.producer.BranchDatabases;
import io.github.mpiumakkho.branchsales.producer.TestcontainersConfiguration;

/** The send round tests against a PostgreSQL branch database, plus two that rename tables (PostgreSQL syntax). */
@Import({ TestcontainersConfiguration.class, BranchDatabases.Postgres.class })
class PostgresSendRoundTest extends AbstractSendRoundTest {

	@Test
	void branchWithoutReturnTablesStillSendsItsSales() {
		// A branch whose back-office was not upgraded yet (or whose login cannot read the return tables)
		LocalDate day = LocalDate.now(ZoneId.of("Asia/Bangkok")).minusDays(3);
		jdbc.sql("insert into daily_sales values (?, 'BR0001', 'CONFIRMED', 1, now())").param(day).update();
		jdbc.sql("insert into daily_sales_line values (?, 'BEV', 10.00, 1)").param(day).update();
		jdbc.sql("alter table daily_return_line rename to daily_return_line_x").update();
		jdbc.sql("alter table daily_return rename to daily_return_x").update();
		try {
			assertThat(round.run()).isEqualTo(new SendRound.Result(1, 1, 0));
		}
		finally {
			jdbc.sql("alter table daily_return_x rename to daily_return").update();
			jdbc.sql("alter table daily_return_line_x rename to daily_return_line").update();
		}
	}

	@Test
	void branchWithoutShiftTablesStillSendsItsSalesAndReturns() {
		// A branch whose POS tables are not there yet (or whose login cannot read them): logged each round
		LocalDate day = LocalDate.now(ZoneId.of("Asia/Bangkok")).minusDays(3);
		jdbc.sql("insert into daily_sales values (?, 'BR0001', 'CONFIRMED', 1, now())").param(day).update();
		jdbc.sql("insert into daily_sales_line values (?, 'BEV', 10.00, 1)").param(day).update();
		jdbc.sql("insert into daily_return values (?, 'BR0001', 'CONFIRMED', 1, now())").param(day).update();
		jdbc.sql("insert into daily_return_line values (?, 'BEV', 1.00, 1)").param(day).update();
		jdbc.sql("alter table pos_shift_tender rename to pos_shift_tender_x").update();
		jdbc.sql("alter table pos_shift rename to pos_shift_x").update();
		try {
			assertThat(round.run()).isEqualTo(new SendRound.Result(2, 2, 0));
		}
		finally {
			jdbc.sql("alter table pos_shift_x rename to pos_shift").update();
			jdbc.sql("alter table pos_shift_tender_x rename to pos_shift_tender").update();
		}
	}
}
