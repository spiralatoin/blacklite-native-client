package com.tersesystems.blacklite.reader;

import org.junit.Test;

public class BlackliteReaderTest {

	@Test
	public void testReaderWithWhere() {
		// 1604460131
		// 1604460129
		String afterDate = "2020-11-04 3:22:09 Z";
		String beforeDate = "2020-11-04 3:22:11 Z";
		BlackliteReader.main(
			"--before", beforeDate,
			"--after", afterDate,
			"--where", "level = 10000",
			"-c",
			"-v",
			"/tmp/blacklite/archive.2020-11-03-07-22.669.db");
	}

	@Test
	public void testTimezone() {
		// 1604460131
		// 1604460129
		String afterDate = "2020-11-03 19:22:09";
		String beforeDate = "2020-11-03 19:22:11";
		BlackliteReader.main(
			"--before", beforeDate,
			"--after", afterDate,
			"--count",
			"--timezone", "PST",
			"/tmp/blacklite/archive.2020-11-03-07-22.669.db");
	}
}
