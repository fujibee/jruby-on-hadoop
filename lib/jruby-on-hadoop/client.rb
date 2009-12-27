module JRubyOnHadoop
  JAVA_MAIN_CLASS = 'org.apache.hadoop.ruby.JRubyJobRunner' 

  class Client
    def initialize(init_script='mapred.rb')
      @init_script = init_script
    end

    def run
      exec cmd
    end

    def cmd
      "hadoop jar #{main_jar_path} #{JAVA_MAIN_CLASS}" +
      " -libjars #{jruby_jar_paths} -files #{@init_script}"
    end

    def main_jar_path
      JRubyOnHadoop.jar_path
    end

    def jruby_jar_paths
      [JRubyJars.core_jar_path, JRubyJars.stdlib_jar_path].join(',')
    end
  end
end
