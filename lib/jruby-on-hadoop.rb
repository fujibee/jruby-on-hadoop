require 'jruby-jars'
require 'jruby-on-hadoop/client'

module JRubyOnHadoop
  def self.jar_path
    File.join(File.expand_path(File.dirname(__FILE__)), "hadoop-ruby.jar")
  end
end
