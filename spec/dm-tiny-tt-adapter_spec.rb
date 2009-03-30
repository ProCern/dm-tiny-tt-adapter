require File.dirname(__FILE__) + '/spec_helper'

require 'dm-core/spec/adapter_shared_spec'

require 'uuidtools'
require 'dm-types/uuid'

describe DataMapper::Adapters::TinyTtAdapter do
  before :all do
    @adapter = DataMapper.setup(:default, :adapter   => 'tiny_tt',
                                          :hostname  => 'localhost',
                                          :port      => 1978)

    class ::Observation
      include DataMapper::Resource

      property :metric_uuid,  UUID,       :key => true
      property :timestamp,    Time,       :key => true
      property :value,        Float

    end
  end

  describe 'create' do
    before :all do
      @metric_uuid = UUID.random_create
    end

    it 'should do stuff' do
      Observation.create(:metric_uuid => @metric_uuid,
                         :timestamp => Time.now,
                         :value => 42.0)

    end

  end


end
