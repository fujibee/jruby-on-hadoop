require 'jruby-on-hadoop'

describe JRubyOnHadoop::Client do
  before do
    @client = JRubyOnHadoop::Client.new
  end

  it 'gather necessary jar paths' do
    version_pattern = '[\d\.]*'
    @client.main_jar_path.should include 'hadoop-ruby.jar'

    @client.opt_libjars.should match /jruby\-core\-#{version_pattern}\.jar/
    @client.opt_libjars.should match /jruby\-stdlib\-#{version_pattern}\.jar/
  end

  it 'gather necessary ruby files' do
    @client.opt_files.split(",").should include "mapred.rb"
    @client.opt_files.should match /ruby_wrapper\.rb/
  end

  it 'construct command for running hadoop' do
    path_pattern = '[\w/\-\.,]*'
    @client.cmd.should match /hadoop jar #{path_pattern}hadoop-ruby.jar org.apache.hadoop.ruby.JRubyJobRunner -libjars #{path_pattern}.jar -files mapred.rb/
  end

  it 'can get mapred args' do
    client = JRubyOnHadoop::Client.new(["examples/mapred.rb", "inputs", "outputs"])
    client.mapred_args.should == "--script mapred.rb inputs outputs"
  end

  it 'can parse args' do
    client = JRubyOnHadoop::Client.new(["examples/mapred.rb", "in", "out"])
    client.script.should == 'mapred.rb'
    client.inputs.should == 'in'
    client.outputs.should == 'out'
    client.files.should include 'examples/mapred.rb'
  end

  it 'should raise error if HADOOP_HOME env is not set' do
    saved = ENV['HADOOP_HOME']
    ENV['HADOOP_HOME'] = ''
    begin
      lambda { JRubyOnHadoop::Client.new }.should raise_error
    ensure
      ENV['HADOOP_HOME'] = saved
    end
  end

  it 'can determin bin/hadoop path' do
    @client.hadoop_cmd.should match /hadoop$/
  end

  it 'can determin bin/hadoop path if even no in PATH env var' do
    saved = ENV['PATH']
    begin
      ENV['PATH'] = ''
      ENV['HADOOP_HOME'].should_not be_empty
      client = JRubyOnHadoop::Client.new
      client.hadoop_cmd.should match ENV['HADOOP_HOME']
      client.hadoop_cmd.should match /hadoop$/
    ensure
      ENV['PATH'] = saved
    end
  end

  it 'should raise error if cannot determin bin/hadoop path' do
    saved_path = ENV['PATH']
    saved_home = ENV['HADOOP_HOME']
    begin
      ENV['PATH'] = ''
      lambda { JRubyOnHadoop::Client.new }.should_not raise_error
      ENV['HADOOP_HOME'] = ''
      lambda { JRubyOnHadoop::Client.new }.should raise_error
    ensure
      ENV['PATH'] = saved_path
      ENV['HADOOP_HOME'] = saved_home
    end
  end

  it 'set HADOOP_CLASSPATH env var' do
      client = JRubyOnHadoop::Client.new
      client.hadoop_classpath.should match JRubyJars.core_jar_path
      client.hadoop_classpath.should match JRubyJars.stdlib_jar_path
  end
end
