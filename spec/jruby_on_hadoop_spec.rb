require 'jruby_on_hadoop'

describe JrubyOnHadoop do
  it 'should return jar path' do
    jar_dir = File.join(File.dirname(__FILE__), '..', 'lib')
    jar_path = File.join(File.expand_path(jar_dir), 'hadoop-ruby.jar')
    JrubyOnHadoop.jar_path.should == jar_path
  end
end
