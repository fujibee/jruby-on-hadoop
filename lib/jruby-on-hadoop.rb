require 'jruby-jars'
require 'jruby-on-hadoop/client'

module JRubyOnHadoop
  FULL_PATH_TO_HERE = File.expand_path(File.dirname(__FILE__))

  def self.jar_path
    File.join(FULL_PATH_TO_HERE, "hadoop-ruby.jar")
  end

  def self.wrapper_ruby_file
    File.join(FULL_PATH_TO_HERE, "_wrapper.rb")
  end
end
