require 'jruby-jars'
require 'jruby-on-hadoop/client'

module JRubyOnHadoop

  def self.lib_path
    File.expand_path(File.dirname(__FILE__))
  end

  def self.jar_path
    File.join(lib_path, "hadoop-ruby.jar")
  end

  def self.wrapper_ruby_file
    File.join(lib_path, "ruby_wrapper.rb")
  end
end
