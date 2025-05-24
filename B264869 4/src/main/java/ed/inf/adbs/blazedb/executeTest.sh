#!/bin/bash

## Function to execute all query and test scripts.
execute_all_tests() {
  mvn clean compile assembly:single
  echo -e "\n\n\n\n\n\n\n"
  for index in {1..12}; do
    echo -e "\nExecuting query file: query${index}.sql"
    java -jar target/blazedb-1.0.0-jar-with-dependencies.jar \
      samples/db \
      samples/input/query${index}.sql \
      samples/output/query${index}.csv
  done

  # Run the on additional data
  for i in {1..12}; do
    echo ""
    echo -e "Running test file: queries${i}.sql"
    java -jar target/blazedb-1.0.0-jar-with-dependencies.jar \
      samples/db \
      samples/input/queries${i}.sql \
      samples/output/queries${i}.csv
  done

## Start execution of all tests.
execute_all_tests