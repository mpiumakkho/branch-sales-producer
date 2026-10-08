package io.github.mpiumakkho.branchsales.producer.dto;

/**
 * The kinds of record the branch sends, one topic each (contract/README.md in the consumer repo), in send order. Each
 * is a header row with a revision and its lines (code, amount, quantity). What differs is the key, the back-office
 * tables they are read from, the status that makes a row ready to send, the topic, and the names of the date field and
 * the line code field in the message.
 */
public enum RecordType {

	/** Daily sales ({@code DailySalesSummary}) from {@code daily_sales}. */
	DAILY_SUMMARY(KeyKind.DAY, "saleDate", "sale_date", "daily_sales", "daily_sales_line", "CONFIRMED", "confirmed_at",
			LineKind.CATEGORY),

	/** Daily returns and voids ({@code DailyReturn}) from {@code daily_return}; HQ stores them after the day's sales. */
	DAILY_RETURN(KeyKind.DAY, "returnDate", "return_date", "daily_return", "daily_return_line", "CONFIRMED",
			"confirmed_at", LineKind.CATEGORY),

	/**
	 * Shift close of one POS terminal ({@code ShiftClose}) from {@code pos_shift}, by tender type. Sent when the shift is
	 * CLOSED; {@code closed_at} is its confirmation time. HQ stores it with or without the day's sales.
	 */
	SHIFT_CLOSE(KeyKind.SHIFT, "businessDate", "business_date", "pos_shift", "pos_shift_tender", "CLOSED", "closed_at",
			LineKind.TENDER);

	/** How a record of the type is identified, apart from its revision ({@link RecordKey}). */
	public enum KeyKind {
		/** One business day: the date column. */
		DAY,
		/** One shift of one POS terminal: the date column, {@code terminal_id} and {@code shift_no}. */
		SHIFT
	}

	/**
	 * The lines of a type.
	 *
	 * @param localCodeColumn line column with the back-office's own code, mapped to the HQ code through configuration
	 * @param codeField       name of the HQ code field of a line in the message
	 * @param minLines        fewest lines the contract allows
	 * @param maxLines        most lines the contract allows
	 */
	public record LineKind(String localCodeColumn, String codeField, int minLines, int maxLines) {

		/** Sales or returns by category (contract/categories.md). */
		public static final LineKind CATEGORY = new LineKind("category_code", "categoryCode", 1, 50);

		/** Shift close by tender type (contract/tender-types.md); empty for a shift without transactions. */
		public static final LineKind TENDER = new LineKind("tender_code", "tenderType", 0, 20);
	}

	private final KeyKind keyKind;
	private final String dateField;
	private final String dateColumn;
	private final String table;
	private final String lineTable;
	private final String readyStatus;
	private final String confirmedAtColumn;
	private final LineKind lineKind;

	RecordType(KeyKind keyKind, String dateField, String dateColumn, String table, String lineTable, String readyStatus,
			String confirmedAtColumn, LineKind lineKind) {
		this.keyKind = keyKind;
		this.dateField = dateField;
		this.dateColumn = dateColumn;
		this.table = table;
		this.lineTable = lineTable;
		this.readyStatus = readyStatus;
		this.confirmedAtColumn = confirmedAtColumn;
		this.lineKind = lineKind;
	}

	/** How a record of this type is identified. */
	public KeyKind keyKind() {
		return keyKind;
	}

	/** Name of the business-date field in the message. */
	public String dateField() {
		return dateField;
	}

	/** Business-date column of both tables. */
	public String dateColumn() {
		return dateColumn;
	}

	/** Back-office header table: one row per key. */
	public String table() {
		return table;
	}

	/** Back-office line table of the header table. */
	public String lineTable() {
		return lineTable;
	}

	/** Header {@code status} of the rows that are sent (rule R1); the other statuses are not read. */
	public String readyStatus() {
		return readyStatus;
	}

	/** Header column with the time the row reached {@link #readyStatus()}; sent as {@code confirmedAt}. */
	public String confirmedAtColumn() {
		return confirmedAtColumn;
	}

	/** The lines of this type. */
	public LineKind lineKind() {
		return lineKind;
	}
}
