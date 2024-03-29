Generate migration file:
```
Usage: analayze_schema rails_dir

To make the schema.rb:
  bundle exec rake db:fix_structure
   -> db/schema.rb
```

Migrate psql dump:
```
Usage: migrate infile[.gz] outfile[.gz]

Migrates apartment data to the public schema, transforming all tenanted primary keys
```
