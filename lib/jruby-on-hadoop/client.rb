module JRubyOnHadoop
  JAVA_MAIN_CLASS = 'org.apache.hadoop.ruby.JRubyJobRunner' 

  class Client
    def initialize(argv=[])
      @init_script = argv[0] || 'mapred.rb'
      @args = argv[1..argv.size-1].join(" ") if argv.size > 0
    end

    def run
      exec cmd
    end

    def cmd
      hadoop_home = ENV['HADOOP_HOME']
      raise 'HADOOP_HOME is not set' unless hadoop_home 
      hadoop_cmd = "#{hadoop_home}/bin/hadoop"
      "#{hadoop_cmd} jar #{main_jar_path} #{JAVA_MAIN_CLASS}" +
      " -libjars #{jruby_jar_paths} -files #{@init_script} #{@args}"
    end

    def main_jar_path
      JRubyOnHadoop.jar_path
    end

    def jruby_jar_paths
      [JRubyJars.core_jar_path, JRubyJars.stdlib_jar_path].join(',')
    end
  end
end
