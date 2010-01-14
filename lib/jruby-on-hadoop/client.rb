module JRubyOnHadoop
  JAVA_MAIN_CLASS = 'org.apache.hadoop.ruby.JRubyJobRunner' 

  class Client
    attr_reader :script, :inputs, :outputs, :files

    def initialize(args=[])
      @args = args
      parse_args

      # env get / set and check
      hadoop_home and hadoop_cmd and hadoop_classpath
    end

    def hadoop_home
      home = ENV['HADOOP_HOME']
      raise 'HADOOP_HOME is not set' if home.nil? or home.empty?
      home
    end

    def hadoop_cmd
      hadoop = `which hadoop 2>/dev/null`
      hadoop = "#{hadoop_home}/bin/hadoop" if hadoop.nil? or hadoop.empty?
      raise 'cannot find hadoop command' unless hadoop
      hadoop.chomp
    end

    def hadoop_classpath
      ENV['HADOOP_CLASSPATH'] =
        ([lib_path, File.dirname(@script_path)] + jruby_jars).join(':')
    end

    def run
      exec cmd
    end

    def cmd
      "#{hadoop_cmd} jar #{main_jar_path} #{JAVA_MAIN_CLASS}" +
      " -libjars #{opt_libjars} -files #{opt_files} #{mapred_args}"
    end

    def parse_args
      @script_path = @args.size > 0 ? @args[0] : 'mapred.rb'
      @script = File.basename(@script_path) 
      @inputs = @args[1] if @args.size == 3
      @outputs = @args[2] if @args.size == 3
      @files = [@script_path, JRubyOnHadoop.wrapper_ruby_file]
    end

    def mapred_args
      args = "--script #{@script} "
      args += "#{@inputs} " if @inputs
      args += "#{@outputs}" if @outputs
      args
    end

    def jruby_jars
      [JRubyJars.core_jar_path, JRubyJars.stdlib_jar_path]
    end

    def opt_libjars; jruby_jars.join(',') end
    def opt_files; @files.join(',') end
    def main_jar_path; JRubyOnHadoop.jar_path end
    def lib_path; JRubyOnHadoop.lib_path end
  end
end
