
require 'rubygems'
require 'rake'

require 'lib/dm-tiny-tt-adapter'

def with_gem(gemname, &blk)
  begin
    require gemname
    blk.call
  rescue LoadError => e
    puts "Failed to load gem #{gemname} because #{e}."
  end
end

with_gem 'echoe' do
  Echoe.new('dm-tiny-tt-adapter',
            DataMapper::Adapters::TokyoTyrantAdapter::VERSION) do |p|
    
    p.author = "Paul Sadauskas"
    p.summary = "An Adapter for Datamapper for TokyoTyrant - Space Optimized"
    p.url = "http://github.com/paul/dm-tiny-tt-adapter"
    p.runtime_dependencies = ['dm-core >= 0.10.0', 'json', 'dm-tokyotyrant-adapter']
    p.development_dependencies = ['rspec', 'echoe', 'yard']
  end
end

with_gem 'spec/rake/spectask' do
  
  desc 'Run all specs'
  Spec::Rake::SpecTask.new(:spec) do |t|
    t.spec_opts << '--options' << 'spec/spec.opts' if File.exists?('spec/spec.opts')
    t.libs << 'lib'
    t.spec_files = FileList['spec/**_spec.rb'] 
  end

  desc 'Default: Run Specs'
  task :default => :spec

  desc 'Run all tests'
  task :test => :spec

end

with_gem 'yard' do
  desc "Generate Yardoc"
  YARD::Rake::YardocTask.new do |t|
    t.files = ['lib/**/*.rb', 'README.markdown']
  end
end


