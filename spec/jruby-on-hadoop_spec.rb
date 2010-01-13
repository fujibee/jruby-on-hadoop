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
