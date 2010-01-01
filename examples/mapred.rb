# wordcount example

def setup(conf)
  ['test/inputs', 'test/outputs']
end

def map(key, value, output, reporter)
  value.split.each do |word|
    output.collect(word, 1)
  end
end

def reduce(key, values, output, reporter)
  sum = 0
  values.each {|v| sum += v }
  output.collect(key, sum)
end
