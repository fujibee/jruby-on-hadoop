package org.apache.hadoop.ruby;

import javax.script.ScriptException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.ruby.mapred.JRubyMapper;
import org.apache.hadoop.ruby.mapred.JRubyReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JRubyJobRunner extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		CommandLineParser parser = new GnuParser();
		Options options = new Options();
		options.addOption(new Option("script", true, "ruby script"));
		options
				.addOption(new Option("dslfile", true, "hadoop ruby DSL script"));

		CommandLine commandLine = parser.parse(options, args);
		JobConf conf = new JobConf(getConf(), JRubyJobRunner.class);
		conf.setJobName("ruby.runner");

		if (commandLine.hasOption("script")) {
			conf.set("mapred.ruby.script", commandLine.getOptionValue("script",
					"mapred.rb"));
		}
		if (commandLine.hasOption("dslfile")) {
			conf.set("mapred.ruby.dslfile", commandLine
					.getOptionValue("dslfile"));
		}
//		System.out.println(options.toString());
//		System.out.println(conf.get("mapred.ruby.script"));
//		System.out.println(conf.get("mapred.ruby.dslfile"));

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(JRubyMapper.class);
		conf.setCombinerClass(JRubyReducer.class);
		conf.setReducerClass(JRubyReducer.class);

		String[] otherArgs = commandLine.getArgs();
		if (otherArgs.length >= 2) {
			FileInputFormat.setInputPaths(conf, otherArgs[0]);
			FileOutputFormat.setOutputPath(conf, new Path(otherArgs[1]));
		}

		// override by Ruby script
		JRubyEvaluator evaluator = new JRubyEvaluator(conf);
		try {
			Object[] paths = (Object[]) evaluator.invoke("wrap_setup", conf);
			if (paths != null && paths.length == 2) {
				FileInputFormat.setInputPaths(conf, (String) paths[0]);
				FileOutputFormat.setOutputPath(conf, new Path((String) paths[1]));
			}
		} catch (ScriptException e) {
			// do nothing. maybe user script has no "setup" method
		}

		JobClient.runJob(conf);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new JRubyJobRunner(),
				args);
		System.exit(res);
	}
}
