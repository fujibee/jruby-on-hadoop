require 'jruby-on-hadoop'

describe JRubyOnHadoop do
  it 'should return jar path' do
    jar_dir = File.join(File.dirname(__FILE__), '..', 'lib')
    jar_path = File.join(File.expand_path(jar_dir), 'hadoop-ruby.jar')
    JRubyOnHadoop.jar_path.should == jar_path
  end
end

describe JRubyOnHadoop::Client do
  it 'gather necessary jar paths' do
    version_pattern = '[\d\.]*'
    client = JRubyOnHadoop::Client.new
    client.main_jar_path.should include 'hadoop-ruby.jar'

    client.jruby_jar_paths.should match /jruby\-core\-#{version_pattern}\.jar/
    client.jruby_jar_paths.should match /jruby\-stdlib\-#{version_pattern}\.jar/
  end

  it 'construct command for running hadoop' do
    path_pattern = '[\w/\-\.,]*'
    client = JRubyOnHadoop::Client.new
    client.cmd.should match /hadoop jar #{path_pattern}hadoop-ruby.jar org.apache.hadoop.ruby.JRubyJobRunner -libjars #{path_pattern}.jar -files mapred.rb/
  end
end
