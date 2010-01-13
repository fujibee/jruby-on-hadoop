require 'ruby_wrapper'

describe 'wrapper' do
  before do
    examples_dir = File.expand_path(File.join(File.dirname(__FILE__), '..', 'examples'))
    $: << examples_dir

    @script = 'wordcount.rb'
    @output, @repoter = mock('output'), mock('repoter') 

    @key, @value = Text.new, Text.new
    @key.set('key')
    @value.set('value')
  end

  it 'can wrap setup' do
    wrap_setup(mock('conf'), @script, nil)
  end

  it 'can wrap mapper' do
    @output.should_receive(:collect).once
    wrap_map(@key, @value, @output, @reporter, @script, nil)
  end

  it 'can wrap reducer' do
    @output.should_receive(:collect).once
    values = [1, 2, 3].map {|v| IntWritable.new(v)}.to_java
    wrap_reduce(@key, values, @output, @reporter, @script, nil)
  end
end
