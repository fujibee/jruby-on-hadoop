package org.apache.hadoop.ruby;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

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

	public Object invoke(String methodName, Object conf) throws ScriptException {
		Object result = null;
		try {
			rubyEngine.eval(readRubyInitialFile());
			result = ((Invocable) rubyEngine).invokeFunction(methodName, conf,
					scriptName);
		} catch (FileNotFoundException e) {
			throw new ScriptException(e);
		} catch (NoSuchMethodException e) {
			throw new ScriptException(e);
		}
		return result;
	}

	public Object invoke(String methodName, Object key, Object value,
			Object output, Object reporter) throws ScriptException {
		Object result = null;
		try {
			rubyEngine.eval(readRubyInitialFile());
			result = ((Invocable) rubyEngine).invokeFunction(methodName, key,
					value, output, reporter, scriptName);
		} catch (FileNotFoundException e) {
			throw new ScriptException(e);
		} catch (NoSuchMethodException e) {
			throw new ScriptException(e);
		}
		return result;
	}

	private Reader readRubyInitialFile() throws FileNotFoundException {
		String initialFile = "init.rb"; // TODO externalize
		InputStream is = Thread.currentThread().getContextClassLoader()
				.getResourceAsStream(initialFile);
		if (is == null) {
			throw new FileNotFoundException(initialFile);
		}
		return new InputStreamReader(is);
	}
}
