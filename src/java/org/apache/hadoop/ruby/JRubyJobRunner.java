package org.apache.hadoop.ruby;

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
		JobConf conf = new JobConf(getConf(), JRubyJobRunner.class);
		conf.setJobName("ruby.runner");

		conf.set("mapred.ruby.script", args[0]);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(JRubyMapper.class);
		conf.setCombinerClass(JRubyReducer.class);
		conf.setReducerClass(JRubyReducer.class);

		if (args.length == 3) {
			FileInputFormat.setInputPaths(conf, args[1]);
			FileOutputFormat.setOutputPath(conf, new Path(args[2]));
		}

		// override by Ruby script
		JRubyEvaluator evaluator = new JRubyEvaluator(conf);
		Object[] paths = (Object[]) evaluator.invoke("setup", conf);
		if (paths != null && paths.length == 2) {
			FileInputFormat.setInputPaths(conf, (String) paths[0]);
			FileOutputFormat.setOutputPath(conf, new Path((String) paths[1]));
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
