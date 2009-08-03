require File.dirname(__FILE__) + '/spec_helper'

require 'uuidtools'

require 'dm-types/uuid'
require 'dm-core/core_ext/symbol'

DataMapper.setup(:default, :adapter   => 'tiny_tt',
                 :hostname  => 'localhost',
                 :port      => 1978)

# DataMapper::Logger.new(STDOUT, 0)

class Datapoint
  include DataMapper::Resource

  property :metric_id,  UUID,  :key => true
  property :timestamp,  Time,  :key => true
  property :value,      Variant
end

describe DataMapper::Adapters::TinyTtAdapter do
  before do
    @metric_id = UUIDTools::UUID.random_create.to_s
    @now = Time.at(1244656800) # noon, june 10
  end

  after do
    # Clear out the DB
    DataMapper.repository(:default).adapter.db do |db|
      db.vanish
    end
  end

  def create_datapoint(args = {})
    attrs = {
      :metric_id => @metric_id,
      :timestamp => @now, 
      :value     => 42.0
    }.merge(args)

    Datapoint.create(attrs)
  end


  describe "Finding datapoints" do
    before do
      @day_ago        = create_datapoint(:timestamp => @now - 86400)
      @day_from_now   = create_datapoint(:timestamp => @now + 86400)
      @hour_ago       = create_datapoint(:timestamp => @now - 3600)
      @hour_from_now  = create_datapoint(:timestamp => @now + 3600)
      @min_ago        = create_datapoint(:timestamp => @now - 60)
      @min_from_now   = create_datapoint(:timestamp => @now + 60)
      @current        = create_datapoint(:timestamp => @now)
    end

    describe "for an exact timestamp" do

      it "should find the datapoint for the exact time" do
        dp = Datapoint.get(@metric_id, @now)
        dp.should == @current
      end

      it "should not find the datapoint for a different time" do
        Datapoint.get(@metric_id, @now+1).should be_nil
        Datapoint.get(@metric_id, @now-10).should be_nil
      end

    end

    describe "multiple metrics" do
      before do
        @another_metric_id = UUIDTools::UUID.random_create.to_s
        create_datapoint(:metric_id => @another_metric_id)

        @result = Datapoint.all(:metric_id => [@metric_id, @another_metric_id],
                                :timestamp => @now)
      end

      it "should be able to retrieve datapoints from both metrics" do
        @result.should have(2).items
      end

    end

    describe "for a time range" do
      before do
        @dps = Datapoint.all(:metric_id => @metric_id,
                             :timestamp => [@now-100, @now+100])
      end

      it 'should find the datapoints within that range' do
        @dps.should include(@min_ago)
        @dps.should include(@current)
        @dps.should include(@min_from_now)
      end

      it 'should not find datapoints outside the range' do
        @dps.should_not include(@day_ago)
        @dps.should_not include(@hour_ago)
        @dps.should_not include(@hour_from_now)
        @dps.should_not include(@day_from_now)
      end

    end

  end

  describe 'Datapoint with numeric value' do

    describe 'create' do

      it 'should create a record' do
        Datapoint.create(:metric_id => @metric_id,
                         :timestamp => @now,
                         :value => 42.0)

        dp = Datapoint.get(@metric_id, @now)
        dp.should_not be_nil
        dp.value.should == 42.0
      end

    end

  end

  describe 'Datapoint with string value' do

    describe 'create' do
      it 'should create a record' do
        Datapoint.create(:metric_id => @metric_id,
                         :timestamp => @now,
                         :value => 'Lorem Ipsum')

        dp = Datapoint.get(@metric_id, @now)
        dp.should_not be_nil
        dp.value.should == 'Lorem Ipsum'
      end

    end

  end


end
