module JRubyOnHadoop
  JAVA_MAIN_CLASS = 'org.apache.hadoop.ruby.JRubyJobRunner' 

  class Client
    def initialize(argv=[])
      @init_script = argv[0] || 'mapred.rb'
      @args = argv[1..argv.size-1].join(" ") if argv.size > 0

      # env check
      hadoop_home = ENV['HADOOP_HOME']
      raise 'HADOOP_HOME is not set' unless hadoop_home 
      @hadoop_cmd = "#{hadoop_home}/bin/hadoop"
      ENV['HADOOP_CLASSPATH'] = '.'
    end

    def run
      exec cmd
    end

    def cmd
      "#{@hadoop_cmd} jar #{main_jar_path} #{JAVA_MAIN_CLASS}" +
      " -libjars #{opt_libjars} -files #{opt_files} #{@args}"
    end

    def opt_libjars
      # jruby jars
      [JRubyJars.core_jar_path, JRubyJars.stdlib_jar_path].join(',')
    end

    def opt_files
      [@init_script, JRubyOnHadoop.wrapper_ruby_file].join(',')
    end

    def main_jar_path
      JRubyOnHadoop.jar_path
    end
  end
end
