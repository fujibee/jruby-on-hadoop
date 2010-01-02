require 'jruby-on-hadoop'

describe JRubyOnHadoop do
  it 'should return jar path' do
    jar_dir = File.join(File.dirname(__FILE__), '..', 'lib')
    jar_path = File.join(File.expand_path(jar_dir), 'hadoop-ruby.jar')
    JRubyOnHadoop.jar_path.should == jar_path
  end

  it 'should return lib path' do
    lib_dir = File.expand_path(File.join(File.dirname(__FILE__), '..', 'lib'))
    JRubyOnHadoop.lib_path.should == lib_dir
  end

  it 'should return wrapper ruby file' do
    dir = File.join(File.dirname(__FILE__), '..', 'lib')
    path = File.join(File.expand_path(dir), 'ruby_wrapper.rb')
    JRubyOnHadoop.wrapper_ruby_file.should == path
  end
end

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
    @client.opt_files.should match /mapred.rb/
  end

  it 'construct command for running hadoop' do
    path_pattern = '[\w/\-\.,]*'
    @client.cmd.should match /hadoop jar #{path_pattern}hadoop-ruby.jar org.apache.hadoop.ruby.JRubyJobRunner -libjars #{path_pattern}.jar -files mapred.rb/
  end

  it 'can get mapred args' do
    client = JRubyOnHadoop::Client.new(["mapred.rb", "inputs", "outputs"])
    client.mapred_args.should == "--script mapred.rb inputs outputs"
  end
end
