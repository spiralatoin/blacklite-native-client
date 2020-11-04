package com.tersesystems.blacklite.reader;

import com.joestelmach.natty.DateGroup;
import com.joestelmach.natty.Parser;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.*;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.TimeZone;

@Command(name = "blacklite-reader",
	version = "1.0",
	description = "Outputs content from blacklite database")
public class BlackliteReader implements Runnable {

	@Parameters(paramLabel = "FILE", description = "one or more files to read")
	File file;

	@Option(names = { "--charset" },
		paramLabel = "CHARSET",
		defaultValue = "utf8",
		description = "Charset (default: ${DEFAULT-VALUE})")
	Charset charset;

	@Option(names = { "--binary" },
		paramLabel = "BINARY",
		description = "Renders content as raw BLOB binary")
	boolean binary;

	@Option(names = { "-b", "--before" },
		paramLabel = "BEFORE",
		description = "Only render entries before the given date")
	String beforeString;

	@Option(names = { "-a", "--after" },
		paramLabel = "AFTER",
		description = "Only render entries after the given date")
	String afterString;

	@Option(names = { "-c", "--count" }, paramLabel = "COUNT", description = "Return a count of entries")
	boolean count;

	@Option(names = { "-v", "--verbose" }, paramLabel = "VERBOSE", description = "Print verbose logging")
	boolean verbose;

	@Option(names = { "-w", "--where" }, paramLabel = "WHERE", description = "Custom SQL where clause")
	String whereString;

	@Option(names = { "-t", "--timezone" }, defaultValue = "UTC", description = "Use the given timezone for before/after dates")
	TimeZone timezone;

	@Option(names = {"-V", "--version"}, versionHelp = true, description = "display version info")
	boolean versionInfoRequested;

	@Option(names = {"-h", "--help"}, usageHelp = true, description = "display this help message")
	boolean usageHelpRequested;

	// this example implements Callable, so parsing, error handling and handling user
	// requests for usage help or version help can be done with one line of code.
	public static void main(String... args) {
		final CommandLine commandLine = new CommandLine(new BlackliteReader());
		if (commandLine.isUsageHelpRequested()) {
			commandLine.usage(System.out);
			return;
		} else if (commandLine.isVersionHelpRequested()) {
			commandLine.printVersionHelp(System.out);
			return;
		}
		System.exit(commandLine.execute(args));
	}

	class ResultConsumer {
		private void print(String decoded) {
			System.out.printf("%s", decoded);
		}

		public void content(byte[] content) {
			if (binary) {
				try {
					System.out.write(content);
				} catch (IOException e) {
					e.printStackTrace();
				}
			} else {
				print(new String(content, charset));
			}
		}

		public void count(long rowCount) {
			print(rowCount + "\n");
		}
	}

	public void run() {
		String url = "jdbc:sqlite:" + file.getAbsolutePath();
		try (Connection c = createConnection(url)) {
			QueryBuilder qb = new QueryBuilder(verbose);

			if (count) {
				qb.addCount(count);
			}

			if (beforeString != null) {
				Instant before = parse(beforeString).orElseThrow(() -> {
					return new IllegalStateException("Cannot parse before string: " + afterString);
				});
				qb.addBefore(before);
			}

			if (afterString != null) {
				Instant after = parse(afterString).orElseThrow(() -> {
					return new IllegalStateException("Cannot parse after string: " + afterString);
				});
				qb.addAfter(after);
			}

			if (whereString != null) {
				qb.addWhere(whereString);
			}

			ResultConsumer consumer = new ResultConsumer();
			qb.execute(c, consumer);
		} catch (SQLException e) {
			e.printStackTrace();
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

	private Connection createConnection(String url) throws SQLException {
		return DriverManager.getConnection(url);
	}

}
