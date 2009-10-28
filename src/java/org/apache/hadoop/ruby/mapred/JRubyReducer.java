package org.apache.hadoop.ruby.mapred;

import java.io.IOException;
import java.util.Iterator;

import javax.script.ScriptException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.ruby.JRubyEvaluator;

public class JRubyReducer extends JRubyMapRed implements
		Reducer<Text, IntWritable, Text, IntWritable> {

	public void reduce(Text key, Iterator<IntWritable> values,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		// invoke "reduce" method in ruby
		JRubyEvaluator evaluator = getJRubyEvaluator();
		try {
			evaluator.invoke("reduce", key, values, output, reporter);
		} catch (ScriptException e) {
			reporter.setStatus(e.getMessage());
		}
	}
}
