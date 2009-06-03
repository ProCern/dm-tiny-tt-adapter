
require 'dm-core'
require 'dm-core/adapters/abstract_adapter'
require 'dm-tokyotyrant-adapter'

module DataMapper::Adapters

  class TinyTtAdapter < TokyoTyrantAdapter

    undef :update, :delete

    def create(resources)
      db do |db|
        resources.each do |resource|
          save(db, key(resource), serialize(resource))
        end
      end
    end

    def read(query)
      metric_id, start_time, end_time = parse_query(query)

      db do |db|
        records = []
        each_day(start_time, end_time) do |timestamp|
          key = key(metric_id, timestamp)
          values = db.get(key)
          records << deserialize(values, metric_id) if values
        end
        query.filter_records(records.flatten)
      end
    end

    protected

    def each_day(from, to, &blk)
      begin
        yield from
        from += (24 * 60 * 60)
      end until from > to
    end

    def parse_query(query)
      uuid = nil
      start_time = end_time = Time.now

      conditions = query.conditions
      conditions.operands.each do |op|
        if op.property.name == :metric_id
          uuid = op.value
        end

        if op.property.name == :timestamp 
          case op
          when DataMapper::Conditions::EqualToComparison
            start_time = end_time = op.value
          when DataMapper::Conditions::InclusionComparison
            start_time, end_time = op.value.begin, op.value.end
          when DataMapper::Conditions::GreaterThanComparison,
               DataMapper::Conditions::GreaterThanOrEqualToComparison
            start_time = op.value
          else
            raise ArgumentError, "#{op.class.inspect} not supported"
          end
        end
      end

      return uuid, start_time, end_time
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
      [resource.timestamp.to_i, resource.value].pack('Id')
    end

    def deserialize(string, metric_id)
      data = []
      data << string.slice!(0,12).unpack('Id') until string.empty?
      data.map { |d| {"timestamp" => Time.at(d[0]), "value" => d[1], "metric_id" => metric_id} }
    end

    def save(db, key, value)
      if !db.putcat(key, value)
        ecode = db.ecode
        raise WriteError, db.errmsg(ecode)
      end
    end


  end

end
