require 'java'

import 'org.apache.hadoop.io.IntWritable'
import 'org.apache.hadoop.io.LongWritable'
import 'org.apache.hadoop.io.Text'

def wrap_setup(conf, script, dslfile)
  require script
  paths = dslfile ? setup(conf, dslfile) : setup(conf)
  paths.to_java if paths
end

def wrap_map(key, value, output, reporter, script, dslfile)
  require script
  output_wrapper = OutputWrapper.new(output)
  dslfile ? 
    map(to_ruby(key), to_ruby(value), output_wrapper, reporter, dslfile) :
    map(to_ruby(key), to_ruby(value), output_wrapper, reporter)
end

def wrap_reduce(key, values, output, reporter, script, dslfile)
  require script
  output_wrapper = OutputWrapper.new(output)
  dslfile ?
    reduce(to_ruby(key), to_ruby(values), output_wrapper, reporter, dslfile) :
    reduce(to_ruby(key), to_ruby(values), output_wrapper, reporter)
end

class OutputWrapper
  def initialize(output)
    @output = output
  end

  def collect(key, value)
    @output.collect(to_java(key), to_java(value))
  end
end

def to_ruby(value)
  case value
  when IntWritable, LongWritable then value.get
  when Text then value.to_string
  else 
    # for Java array
    if value.respond_to? :map
      value.map {|v| to_ruby(v)}
    else value # as is
    end
  end
end

def to_java(value)
  case value
  when Integer then IntWritable.new(value)
  when String then t = Text.new; t.set(value); t
  when Array then value.to_java
  else raise "no match class: #{value.class}"
  end
end
