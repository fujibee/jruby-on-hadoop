package org.apache.hadoop.ruby.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

public class JRubyReducerTest {

	@Test
	public void testReduce() throws IOException {
		Text key = new Text();
		key.set("key");
		List<IntWritable> values = new ArrayList<IntWritable>();
		values.add(new IntWritable(1));
		values.add(new IntWritable(2));
		values.add(new IntWritable(3));

		JRubyReducer reducer = new JRubyReducer();
		JobConf conf = new JobConf();
		conf.set("mapred.ruby.script", "mapred.rb");
		reducer.configure(conf);

		try {
			reducer.reduce(key, values.iterator(), null, null);
		} catch (Throwable t) {
			// ignore
			// TODO mock check
		}
	}

}
