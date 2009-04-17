task :codegen do
  sh 'ruby protocol/codegen.rb > lib/amqp/spec.rb'
  sh 'ruby lib/amqp/spec.rb'
end

task :spec do
  sh 'bacon lib/amqp.rb'
end

desc 'Creates the gem file'
task :gem do
  sh 'gem build *.gemspec'
end

namespace :gem do
  desc 'Creates the gem and installs it locally'
  task :install => :gem do
    sh 'gem install dougbarth-sync_mq-amqp-*.gem'
  end
end

task :pkg => :gem
task :package => :gem
