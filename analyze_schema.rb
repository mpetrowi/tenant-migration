#!/bin/ruby

# Usage: analayze_schema rails_dir
#
#  To make the schema.rb:
#    bundle exec rake db:fix_structure
#     -> db/schema.rb
#
# Migrates apartment data to the public schema, transforming all tenanted primary keys

require 'active_support/inflector'
require 'zlib'

path = ARGV[0] || '.'

init = File.open(path + "/config/initializers/apartment.rb").read
if !File.file?(path + "/db/schema.rb")
  puts "Missing schema, run  bundle exec rake db:fix_structure"
  exit
end

schema = File.open(path + "/db/schema.rb").read

global_models = init.split("\n").select do |line|
  line.include?("config.excluded_models")..line.include?("}") ? true : false
end
global_models = (global_models[1..-2]).map(&:strip)
global_tables = global_models.map { |t| ActiveSupport::Inflector.tableize(t).tr('/', '_') }

puts <<EOF
class AddTenantId < ActiveRecord::Migration[7.0]
  def change
    tenant_id = TenantMigrationSupport::current_apartment_tenant&.id

    opts = {
      foreign_key: { to_table: "public.tenants", on_delete: :cascade },
      default: tenant_id,
      null: tenant_id.nil?,
    }

EOF

table = ''
schema.split("\n").grep(/create_table|t\.index .* unique: true/) do |line|
  if m = line.match(/create_table "(?<table>[^"]*)"/)
    table = m[:table]
    if !global_tables.include?(table) && !table.match?(/^que_/)
      puts <<EOF

    # #{table}
    add_column :#{table}, :tenant_id, :bigint, **opts
    add_index :#{table}, :tenant_id
EOF
    end
  elsif !global_tables.include?(table) && !table.match?(/^que_/)
    m = line.match(/t\.index (?<cols>\[.*\]), name: "(?<name>[^"]*)", unique: true(?<rest>.*)$/)
    raise "invalid: #{line}" if !m

    puts <<EOF
    remove_index :#{table}, column: #{m[:cols]}, name: :#{m[:name]}, unique: true#{m[:rest]}
    add_index :#{table}, #{m[:cols]}, name: :#{m[:name]}, unique: true#{m[:rest]}
EOF
  end
end

puts <<EOF
  end
end
EOF

