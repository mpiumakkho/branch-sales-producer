package io.github.mpiumakkho.branchsales.producer.dto;

/**
 * The kinds of record the branch sends, one topic each (contract/README.md in the consumer repo). Both have the
 * same shape: one business day by category with a revision. What differs is the back-office tables they are read
 * from, the topic, and the name of the date field in the message.
 */
public enum RecordType {

	/** Daily sales ({@code DailySalesSummary}) from {@code daily_sales}. */
	DAILY_SUMMARY("saleDate", "daily_sales", "daily_sales_line", "sale_date"),

	/** Daily returns and voids ({@code DailyReturn}) from {@code daily_return}; HQ stores them after the day's sales. */
	DAILY_RETURN("returnDate", "daily_return", "daily_return_line", "return_date");

	private final String dateField;
	private final String table;
	private final String lineTable;
	private final String dateColumn;

	RecordType(String dateField, String table, String lineTable, String dateColumn) {
		this.dateField = dateField;
		this.table = table;
		this.lineTable = lineTable;
		this.dateColumn = dateColumn;
	}

	/** Name of the business-date field in the message. */
	public String dateField() {
		return dateField;
	}

	/** Back-office header table: one row per business day. */
	public String table() {
		return table;
	}

	/** Back-office line table of the header table. */
	public String lineTable() {
		return lineTable;
	}

	/** Business-date column of both tables. */
	public String dateColumn() {
		return dateColumn;
	}
}
