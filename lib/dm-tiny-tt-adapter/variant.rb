class Variant < DataMapper::Type
  primitive Object

  def self.load(value, property)
    value
  end

  def self.dump(value, property)
    value
  end

  def self.typecast(value, property)
    value
  end

end


