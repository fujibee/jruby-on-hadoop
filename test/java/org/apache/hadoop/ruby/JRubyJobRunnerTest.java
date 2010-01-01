package org.apache.hadoop.ruby;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class JRubyJobRunnerTest {

	@Test
	public void testRun() throws Exception {
		JRubyJobRunner runner = new JRubyJobRunner();
		Configuration conf = new Configuration();
		runner.setConf(conf);
		String[] args = { "--script", "mapred.rb" };
		try {
			runner.run(args);
		} catch (Throwable t) { /* ignore */ }
	}
}
