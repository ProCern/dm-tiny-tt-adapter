
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
      model = query.model

      db do |db|
        keys = db.fwmkeys()
        records = []
        keys.each do |key|
          metric_uuid = key.split('/').first
          value = db.get(key)
          records << deserialize(value, metric_uuid) if value
        end
        filter_records(records, query)
      end
    end

    protected

    def key(resource)
      "#{resource.metric_uuid}/#{resource.timestamp.strftime("%j")}"
    end

    def serialize(resource)
      [resource.timestamp.to_i, resource.value].pack('If')
    end

    def deserialize(string, metric_uuid)
      data = []
      data << string.slice!(0,8).unpack('If') until string.empty?
      data.map { |d| {"timestamp" => d[0], "value" => d[1], "metric_uuid" => metric_uuid} }
    end

    def save(db, key, value)
      if !db.putcat(key, value)
        ecode = db.ecode
        raise WriteError, db.errmsg(ecode)
      end
    end


  end

end
