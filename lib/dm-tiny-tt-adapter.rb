
require 'dm-core'
require 'dm-core/adapters/abstract_adapter'
require 'dm-tokyotyrant-adapter'

require 'hitimes'


module DataMapper::Adapters

  class TinyTtAdapter < TokyoTyrantAdapter

    NUMERIC_TYPE = 0x0
    STRING_TYPE  = 0x1

    undef :update, :delete

    def create(resources)
      db do |db|
        resources.each do |resource|
          save(db, key(resource), serialize(resource))
        end
      end
    end

    def read(query)
      total_timer  = Hitimes::TimedMetric.now("Total #read")
      fetch_timer  = Hitimes::TimedMetric.new("Fetching from TokyoTyrant DB")
      parse_timer  = Hitimes::TimedMetric.new("Parsing documents")
      filter_timer = Hitimes::TimedMetric.new("Filtering records")

      metric_ids, start_time, end_time = parse_query(query)

      records = db do |db|
        records = []
        metric_ids.each do |metric_id|
          each_day(start_time, end_time) do |timestamp|
            key = key(metric_id, timestamp)
            values = fetch_timer.measure { db.get(key) }
            records << parse_timer.measure { deserialize(query.model, values, metric_id) } if values
          end
        end
        filter_timer.measure { query.filter_records(records.flatten) }
      end
      total_timer.stop
      
      DataMapper.logger.info("TTAdapter read: %s: (%i..%i) %0.6f fetching, %0.6f parsing, %0.6f filtering, %0.6f total" % 
                             [metric_ids.inspect, start_time, end_time, 
                               fetch_timer.sum, parse_timer.sum, filter_timer.sum, total_timer.sum])

      records
    end

    protected

    def each_day(from, to, &blk)
      begin
        yield from
        from += (24 * 60 * 60)
      end until from > to
    end

    def parse_query(query)
      uuids = nil
      start_time = end_time = Time.now

      conditions = query.conditions
      conditions.operands.each do |op|
        if op.subject.name == :metric_id
          case value = op.value
          when Array
            uuids = value.map { |v| v.to_s }
          else
            uuids = [value.to_s]
          end
        elsif op.subject.name == :timestamp 
          case op
          when DataMapper::Query::Conditions::EqualToComparison
            start_time = end_time = op.value
          when DataMapper::Query::Conditions::InclusionComparison
            start_time, end_time = op.value[0], op.value[1]
          when DataMapper::Query::Conditions::GreaterThanComparison,
               DataMapper::Query::Conditions::GreaterThanOrEqualToComparison
            start_time = op.value
          else
            raise ArgumentError, "#{op.class.inspect} not supported"
          end
        else
          raise ArgumentError, "Can't query on #{op.subject.type.inspect}"
        end
      end

      return uuids, start_time, end_time
    end

    def key(*args)
      if args.first.is_a?(DataMapper::Resource)
        resource = args.first
        uuid, timestamp = resource.metric_id, resource.timestamp
      else
        uuid, timestamp = *args
      end
      "#{uuid}/#{timestamp.strftime("%Y%m%d")}"
    end

    def serialize(resource)
      type = case resource.value
             when Numeric then NUMERIC_TYPE
             else 
               STRING_TYPE
             end

      timestamp = resource.timestamp.to_i
      value = resource.value.to_s
      length = value.length

      [type, timestamp, length, value].pack('CIIa*')
    end

    def deserialize(model, string, metric_id)
      data = []
      until string.empty?
        type, time, len = string.slice!(0,9).unpack('CII')
        value = string.slice!(0,len)
        value = case type
                when NUMERIC_TYPE then value.to_f
                else 
                  value
                end

        data << {"timestamp" => Time.at(time), "value" => value, "metric_id" => metric_id}
      end
      data
    end

    def save(db, key, value)
      if !db.putcat(key, value)
        ecode = db.ecode
        raise WriteError, db.errmsg(ecode)
      end
    end

  end

end
