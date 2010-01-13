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
end
