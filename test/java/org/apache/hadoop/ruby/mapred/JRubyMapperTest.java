package org.apache.hadoop.ruby.mapred;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

public class JRubyMapperTest {

	@Test
	public void testMap() throws IOException {
		LongWritable key = new LongWritable(0L);
		Text value = new Text();
		value.set("value");

		JRubyMapper mapper = new JRubyMapper();
		JobConf conf = new JobConf();
		conf.set("mapred.ruby.script", "mapred.rb");
		mapper.configure(conf);

		try {
			mapper.map(key, value, null, null);
		} catch (Throwable t) {
			// ignore
			// TODO mock check
		}
	}

}
