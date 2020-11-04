package com.tersesystems.blacklite.reader;

import com.joestelmach.natty.DateGroup;
import com.joestelmach.natty.Parser;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.io.File;
import java.nio.charset.Charset;
import java.sql.*;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.TimeZone;
import java.util.function.Consumer;

@Command(name = "blacklite-reader",
	mixinStandardHelpOptions = true,
	version = "blacklite-reader 1.0",
	description = "Outputs content from blacklite database")
public class BlackliteReader implements Runnable {

	@Parameters(paramLabel = "FILES",
		arity = "1..*",
		description = "one or more files to read")
	File[] files;

	@Option(names = { "--charset" },
		paramLabel = "CHARSET",
		defaultValue = "utf8",
		description = "Charset (default: ${DEFAULT-VALUE})")
	Charset charset;

	@Option(names = { "-b", "--before" },
		paramLabel = "BEFORE",
		description = "Only render entries before the given date")
	String beforeString;

	@Option(names = { "-c", "--count" }, paramLabel = "COUNT", description = "Return a count of entries")
	boolean count;

	@Option(names = { "-a", "--after" },
		paramLabel = "AFTER",
		description = "Only render entries after the given date")
	String afterString;

	@Option(names = { "-v", "--verbose" }, paramLabel = "VERBOSE", description = "Print verbose logging")
	boolean verbose;

	@Option(names = { "-w", "--where" }, paramLabel = "WHERE", description = "Custom SQL where clause")
	String whereString;

	@Option(names = { "-t", "--timezone" }, defaultValue = "UTC")
	TimeZone timezone;

	// this example implements Callable, so parsing, error handling and handling user
	// requests for usage help or version help can be done with one line of code.
	public static void main(String... args) {
		final CommandLine commandLine = new CommandLine(new BlackliteReader());
		System.exit(commandLine.execute(args));
	}

	public void run() {
		for(File file : files) {
			String url = "jdbc:sqlite:" + file.getAbsolutePath();

      // XXX Need to be able to identify compressed content here
			// XXX Need to be able to specify dictionary for zstd content
			ResultConsumer consumer = new ResultConsumer() {
				private void print(String decoded) {
					System.out.printf("%s", decoded);
				}

				@Override
				public void content(byte[] content) {
					print(decode(content));
				}

				@Override
				public void count(long rowCount) {
					print(rowCount + " " + file + "\n");
				}
			};

			try (Connection c = createConnection(url)) {
				QueryBuilder qb = new QueryBuilder(verbose);

				if (count) {
					qb.addCount(count);
				}

				if (beforeString != null) {
					Instant before = parse(beforeString).orElseThrow(() -> new IllegalStateException("Cannot parse before string: " + afterString));;
					qb.addBefore(before);
				}

        if (afterString != null) {
          Instant after =
              parse(afterString)
                  .orElseThrow(() -> new IllegalStateException("Cannot parse after string: " + afterString));
          qb.addAfter(after);
				}

				if (whereString != null) {
					qb.addWhere(whereString);
				}

        qb.execute(c, consumer);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	private Optional<java.time.Instant> parse(String dateString) {
		Parser parser = new Parser(timezone);
		final List<DateGroup> groups = parser.parse(dateString);
		for(DateGroup group:groups) {
			List<java.util.Date> dates = group.getDates();
			if (dates.isEmpty()) {
				return Optional.empty();
			}
			return Optional.of(dates.get(0).toInstant());
		}
		return Optional.empty();
	}

	private String decode(byte[] content) {
		return new String(content, charset);
	}

	private Connection createConnection(String url) throws SQLException {
		return DriverManager.getConnection(url);
	}

	private class QueryBuilder {
		private final boolean verbose;
		private String whereString;
		private Instant before;
		private Instant after;
		private int boundParams = 0;
		private boolean count;

		public QueryBuilder(boolean verbose) {
			this.verbose = verbose;
		}

		public void addBefore(Instant before) {
			this.boundParams += 1;
			this.before = before;
		}

		public void addAfter(Instant after) {
			this.boundParams += 1;
			this.after = after;
		}

		public void addWhere(String whereString) {
			this.whereString = whereString.trim();
		}

		public void addCount(boolean count) {
			this.count = count;
		}

		public void execute(Connection c, ResultConsumer consumer) throws SQLException {
			final String statement = createSQL();
			if (verbose) {
				verbosePrint("QueryBuilder statement: " + statement);
				verbosePrint("QueryBuilder before: " + before + ((before != null) ? " / " + before.getEpochSecond() : ""));
				verbosePrint("QueryBuilder after: " + after + ((after != null) ? " / " + after.getEpochSecond() : ""));
				verbosePrint("QueryBuilder where: " + whereString);
			}

			try (PreparedStatement ps = c.prepareStatement(statement)) {
				int adder = 1;
				if (before != null) {
					ps.setLong(adder++, before.getEpochSecond());
				}

				if (after != null) {
					ps.setLong(adder, after.getEpochSecond());
				}

				try (final ResultSet rs = ps.executeQuery()) {
					if (count) {
						if (rs.next()) {
							consumer.count(rs.getLong(1));
						}
					}

					while (rs.next()) {
						final byte[] content = rs.getBytes("content");
						consumer.content(content);
					}
				}
			}
		}
		private String createSQL() {
			StringBuilder sb = new StringBuilder();
			if (count) {
				// SQLite does a full table scan for count, so use MAX(_rowid_) instead
				sb.append("SELECT MAX(_rowid_) FROM entries");
			} else {
				sb.append("SELECT content FROM entries");
			}

			if (boundParams > 0 || whereString != null) {
				sb.append(" WHERE ");
			}

			if (before != null) {
				sb.append(" timestamp < ? ");
				if (boundParams > 1) {
					sb.append(" AND ");
				}
			}

			if (after != null) {
				sb.append(" timestamp > ? ");
				if (whereString != null && ! whereString.isEmpty()) {
					sb.append(" AND ");
				}
			}

			if (whereString != null && ! whereString.isEmpty()) {
				sb.append(whereString);
			}

			return sb.toString();
		}

	}

	private void verbosePrint(String s) {
		System.err.println(s);
	}

	private abstract class ResultConsumer {

		public abstract void content(byte[] content);

		public abstract void count(long aLong);
	}
}
