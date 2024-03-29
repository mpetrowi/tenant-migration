#!/bin/ruby

# Usage: migrate infile[.gz] outfile[.gz]
#
# Migrates apartment data to the public schema, transforming all tenanted primary keys

require 'active_support/inflector'
require 'zlib'

# additional fk fields that need to be offset
INCLUDE_FKS = ["behavior_id"]

# additional fk fields that need to be left alone
EXCLUDE_FKS = ["context_id", "parent_context_id", "deployment_id", "line_item_id", "client_id"]

ARGV.length == 2 or raise "usage: migrate infile[.gz] outfile[.gz]"

input = File.open(ARGV[0], "rb")
input = Zlib::GzipReader.new(input) if ARGV[0].end_with?(".gz")

output = File.open(ARGV[1], "wb")
output = Zlib::GzipWriter.new(output) if ARGV[1].end_with?(".gz")

# Process COPY blocks in inputs
#
# Yields the COPY params, which retruns a lambda to transform the data to public
def process_copies(input, output = nil)
  input.rewind
  line_no = 0
  while (line = input.gets&.chomp)
    line_no += 1

    if !line.match?(/^COPY/)
      output&.puts line
      next
    end

    m = line.match(/^COPY "?(?<schema>[^".]*)"?\."?(?<table>[^".]*)"? \((?<cols>.*)\) FROM stdin;$/)
    raise "#{line_no}: couldn't parse COPY\n#{line}" if !m

    transform = yield m[:schema], m[:table], *(m[:cols].split(/, */))

    # transform:
    #   nil means no transformation
    #   non-nil is a lambda to transform data to the public schema

    if transform
      output&.puts "COPY public.#{m[:table]} (#{m[:cols]}) FROM stdin;"
    else
      output&.puts line
    end

    while (line = input.gets.chomp)
      line_no += 1
      if line == '\\.'
        output&.puts line
        break
      end

      if transform
        data = line.split("\t", -1)
        raise "#{line_no}: data format error\n#{line}" if data.length < 1

        newdata = transform.call(*data)
        output&.puts newdata.join("\t")
      else
        output&.puts line
      end
    end
  end
rescue Exception => e
  puts "#{line_no}: #{line}"
  raise e
end

tenants = {}
global_tables = []
tenanted_tables = []

puts "Scanning dump"
process_copies(input) do |schema, table, *cols|
  if schema == "public"
    if table == "tenants"
      global_tables << table
      ->(id, schema, offset, *other) { tenants[schema] = { offset:, id:} }
    elsif cols.include?("tenant_id")
      tenanted_tables << table
      nil
    else
      global_tables << table
      nil
    end
  end
end

def output_array(arr)
  arr.map{ |x| "  #{x}" }.join("\n")
end

puts <<~EOF
  Tenants:
  #{output_array tenants.keys}

  Global tables:
  #{output_array global_tables}

  Tenanted tables:
  #{output_array tenanted_tables}
EOF

puts "\nTransmogrifying.. 🪄"
warns = []
process_copies(input, output) do |schema, table, *cols|
  tenant = tenants[schema]
  if schema != "public" && !tenant
    warns << "Skipping schema #{schema}"
    next nil
  end

  offset = tenant ? tenant[:offset].to_i : 0

  if !global_tables.include?(table) && !tenanted_tables.include?(table)
    raise "Unknown table"
  end

  if !cols.include?("id")
    # All of our tables contain an id column. This skips
    # activerecord internal stuff
    next nil
  end

  transforms = cols.map do |col|
    if col == "id"
      if tenanted_tables.include? table
        if tenant == "public"
          ->(_x) { raise "Unexpected public data" }
        else
          ->(x) { Integer(x) + offset }
        end
      end
    elsif col == "tenant_id" && tenant
      ->(x) { x == tenant[:id] ? x : raise("Incorrect tenant_id") }
    elsif col.end_with?("_id")
      ref = col.delete_suffix("_id")
      ref = ActiveSupport::Inflector.pluralize(ref)
      if EXCLUDE_FKS.include?(col)
        nil
      elsif tenanted_tables.include? ref || INCLUDE_FKS.include?(col)
        ->(x) { x == "\\N" ? "\\N" : Integer(x) + offset }
      elsif global_tables.include? ref
        nil
      else
        warns << "Unknown ref #{table}.#{col}"
        nil
      end
    else
      nil
    end
  end

  ->(*data) { data.zip(transforms).map { |d, tr| tr ? tr.call(d) : d } }
end

puts <<~EOF

  Warnings:
  #{output_array warns.uniq}

  DONE
EOF

