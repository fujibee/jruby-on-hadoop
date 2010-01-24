package org.apache.hadoop.ruby;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

import javax.script.ScriptException;

import org.apache.hadoop.mapred.JobConf;
import org.jruby.embed.LocalContextScope;
import org.jruby.embed.ScriptingContainer;

public class JRubyEvaluator {

	private static final String WRAPPER_FILE_NAME = "ruby_wrapper.rb";

	/** invoke count is limited so that memory leaking */
	private static final int INVOKE_LIMIT = 10000;

	private static int invokeCounter = 0;

	private ScriptingContainer rubyEngine;

	/** ruby script name kicked by Java at first */
	private String scriptFileName;

	/** hadoop ruby dsl file using in Hadoop mapper/reducer */
	private String dslFileName;

	public JRubyEvaluator(JobConf conf) {
		scriptFileName = conf.get("mapred.ruby.script");
		dslFileName = conf.get("mapred.ruby.dslfile");

		setupEngine();
	}

	public Object invoke(String methodName, Object conf) throws ScriptException {
		Object self = null; // if receiver is null, should use toplevel.
		Object result = rubyEngine.callMethod(self, methodName, new Object[] {
				conf, scriptFileName, dslFileName }, Object[].class);
		invokeCounter++;
		return result;
	}

	public Object invoke(String methodName, Object key, Object value,
			Object output, Object reporter) throws ScriptException {
		Object self = null; // if receiver is null, should use toplevel.
		Object result = rubyEngine.callMethod(self, methodName, new Object[] {
				key, value, output, reporter, scriptFileName, dslFileName },
				null);
		invokeCounter++;
		return result;
	}

	// check resouce and restart engine if over limit
	public void checkResource() {
		// now simply count because cannot check directly
		// ThreadContextMap in ThreadService seems to be leaked
		if (invokeCounter > INVOKE_LIMIT) {
			invokeCounter = 0;
			setupEngine();
		}
	}

	private void setupEngine() {
		rubyEngine = new ScriptingContainer(LocalContextScope.SINGLETHREAD);
		if (rubyEngine == null)
			throw new RuntimeException("cannot find jruby engine");
		try {
			// evaluate ruby library upfront
			rubyEngine.runScriptlet(readRubyWrapperFile(), WRAPPER_FILE_NAME);
		} catch (Exception e) {
			throw new RuntimeException("cannot find wrapper file", e);
		}
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
