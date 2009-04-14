require File.dirname(__FILE__) + '/spec_helper'

require 'dm-core/spec/adapter_shared_spec'

require 'uuidtools'
require 'dm-types/uuid'
require 'dm-core/core_ext/symbol'

describe DataMapper::Adapters::TinyTtAdapter do
  before :all do
    @adapter = DataMapper.setup(:default, :adapter   => 'tiny_tt',
                                          :hostname  => 'localhost',
                                          :port      => 1978)

    class ::Observation
      include DataMapper::Resource

      property :metric_uuid,  UUID,  :key => true
      property :timestamp,    Time,  :key => true
      property :value,        Float

    end

    @metric_uuid = UUID.random_create

    @now = Time.at(Time.now.to_i) 
    if (hour = @now.hour) > 12
      # normalize things to noon, so if we run this late in the day,
      # today's datapoints don't end up in tomorrow's bucket
      @now -= (hour-12)*60*60
    end
  end

  after :all do
    # Clear out the DB
    @adapter.db do |db|
      db.vanish
    end
  end

  describe 'create' do

    it 'should create a record' do
      Observation.create(:metric_uuid => @metric_uuid,
                         :timestamp => @now,
                         :value => 42.0)

      Observation.get(@metric_uuid, @now).should_not be_nil
    end

  end

  describe 'reading an empty database' do

    it 'should not raise an error' do
      @adapter.db { |db| db.vanish }
      lambda {
        Observation.all(:metric_uuid => @metric_uuid).should be_empty
      }.should_not raise_error
    end

  end

  describe 'read' do
    before :all do
      @yest = @now - (24*60*60)
      @tomm = @now + (24*60*60)
      3.times do |i|
        offset = i * 600
        obs = Observation.create(:metric_uuid => @metric_uuid,
                                 :timestamp => @yest + offset,
                                 :value => offset.to_f)
        instance_variable_set(:"@yest_#{i+1}", obs)

        obs = Observation.create(:metric_uuid => @metric_uuid,
                                 :timestamp => @now + offset,
                                 :value => offset.to_f)
        instance_variable_set(:"@today_#{i+1}", obs)

        obs = Observation.create(:metric_uuid => @metric_uuid,
                                 :timestamp => @tomm + offset,
                                 :value => offset.to_f)
        instance_variable_set(:"@tomm_#{i+1}", obs)
      end

    end

    it 'should be able to retrieve datapoints' do
      Observation.get(@metric_uuid, @now).should == @today_1
    end

    describe 'equal' do
      before :all do
        @result = Observation.all(:metric_uuid => @metric_uuid, :timestamp => @now)
      end

      it 'should return a single datapoint' do
        @result.size.should == 1
      end

      it 'should return the datapoint that matches' do
        @result.should include(@today_1)
      end
    end

    describe 'range' do
      before :all do
        @result = Observation.all(:metric_uuid => @metric_uuid, :timestamp => (@yest..@now))
      end

      it 'should return the datapoints that match' do
        [@yest_1, @yest_2, @yest_3, @today_1].each { |dp|
          @result.should include(dp)
        }
      end

      it 'should return the datapoints that match' do
        [@today_2, @today_3, @tomm_1, @tomm_2, @tomm_3].each { |dp|
          @result.should_not include(dp)
        }
      end
    end

    describe 'greater than or equal to' do
      before :all do
        @result = Observation.all(:metric_uuid => @metric_uuid, :timestamp.gte => @yest)
      end

      it 'should return the datapoints that match' do
        [@yest_1, @yest_2, @yest_3, @today_1, @today_2, @today_3].each { |dp|
          @result.should include(dp)
        }
      end

      it 'should not include datapoints from after midnight tonight' do
        [@tomm_1, @tomm_2, @tomm_3].each { |dp|
          @result.should_not include(dp)
        }
      end
    end

  end

end
