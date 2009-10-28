package org.apache.hadoop.ruby.mapred;

import java.io.IOException;

import javax.script.ScriptException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.ruby.JRubyEvaluator;

public class JRubyMapper extends JRubyMapRed implements
		Mapper<LongWritable, Text, Text, IntWritable> {

	public void map(LongWritable key, Text value,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		// invoke "map" method in ruby
		JRubyEvaluator evaluator = getJRubyEvaluator();
		try {
			evaluator.invoke("map", key, value, output, reporter);
		} catch (ScriptException e) {
			reporter.setStatus(e.getMessage());
		}
	}
}
