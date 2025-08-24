#!/bin/bash

HOST="http://209.192.245.26:8123"
USER="sol"
PASSWORD="sol"
DATABASE="solwich"
OUTPUT_DIR="create_tables"

mkdir -p "$OUTPUT_DIR"

tables=$(curl -s "$HOST?user=$USER&password=$PASSWORD&database=$DATABASE" \
  --data-binary "SELECT name FROM system.tables WHERE database = '$DATABASE'")

echo "Found tables:"
echo "$tables"
echo

for table in $tables; do
    echo "-- Exporting create DDL for table: $table"
    ddl=$(curl -s "$HOST?user=$USER&password=$PASSWORD&database=$DATABASE" \
      --data-binary "SHOW CREATE TABLE \`$table\`")
    echo -e "$ddl" > "$OUTPUT_DIR/${table}.sql"
done

echo "All CREATE TABLE statements saved in $OUTPUT_DIR/"
