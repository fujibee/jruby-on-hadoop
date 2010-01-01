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

	private static final String WRAPPER_FILE_NAME = "ruby_wrapper.rb";

	private ScriptEngine rubyEngine;

	/** ruby script name kicked by Java at first */
	private String scriptFileName;

	/** hadoop ruby dsl file using in Hadoop mapper/reducer */
	private String dslFileName;

	public JRubyEvaluator(JobConf conf) {
		rubyEngine = new ScriptEngineManager().getEngineByName("jruby");
		scriptFileName = conf.get("mapred.ruby.script");
		dslFileName = conf.get("mapred.ruby.dslfile");
	}

	public Object invoke(String methodName, Object conf) throws ScriptException {
		Object result = null;
		try {
			rubyEngine.eval(readRubyWrapperFile());
			result = ((Invocable) rubyEngine).invokeFunction(methodName, conf,
					scriptFileName, dslFileName);
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
			rubyEngine.eval(readRubyWrapperFile());
			result = ((Invocable) rubyEngine).invokeFunction(methodName, key,
					value, output, reporter, scriptFileName, dslFileName);
		} catch (FileNotFoundException e) {
			throw new ScriptException(e);
		} catch (NoSuchMethodException e) {
			throw new ScriptException(e);
		}
		return result;
	}

	private Reader readRubyWrapperFile() throws FileNotFoundException {
		InputStream is = Thread.currentThread().getContextClassLoader()
				.getResourceAsStream(WRAPPER_FILE_NAME);
		if (is == null) {
			throw new FileNotFoundException(WRAPPER_FILE_NAME);
		}
		return new InputStreamReader(is);
	}
}
