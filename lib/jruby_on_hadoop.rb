require 'jruby_on_hadoop/hadoop_client'

module JrubyOnHadoop
  def self.jar_path
    File.join(File.expand_path(File.dirname(__FILE__), "hadoop-ruby.jar"))
  end
end
