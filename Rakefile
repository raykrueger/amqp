require 'rake/testtask'

task :codegen do
  sh 'ruby protocol/codegen.rb > lib/amqp/spec.rb'
  sh 'ruby lib/amqp/spec.rb'
end

task :spec do
  sh 'bacon lib/amqp.rb'
end

task :gem do
  sh 'gem build *.gemspec'
end

Rake::TestTask.new do |t|
  t.libs << "test"
  t.pattern = 'test/**/*_test.rb'
  t.verbose = true
end

task :pkg => :gem
task :package => :gem
