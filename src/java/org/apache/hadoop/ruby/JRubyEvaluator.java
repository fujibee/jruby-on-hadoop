package org.apache.hadoop.ruby;

import java.io.FileNotFoundException;
import java.io.FileReader;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.apache.hadoop.mapred.JobConf;

public class JRubyEvaluator {

	private ScriptEngine rubyEngine;
	private String scriptName;

	public JRubyEvaluator(JobConf conf) {
		rubyEngine = new ScriptEngineManager().getEngineByName("jruby");
		scriptName = conf.get("mapred.ruby.script");
	}

	public Object invoke(String methodName, Object key, Object value,
			Object output, Object reporter) throws ScriptException {
		Object result = null;
		try {
			rubyEngine.eval(new FileReader("init.rb"));
			result = ((Invocable) rubyEngine).invokeFunction(methodName,
					scriptName, key, value, output, reporter);
		} catch (FileNotFoundException e) {
			throw new ScriptException(e);
		} catch (NoSuchMethodException e) {
			throw new ScriptException(e);
		}
		return result;
	}
}
