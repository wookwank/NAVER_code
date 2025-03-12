#!/bin/bash

# Number of times to run `go test`
NUM_RUNS=10

# Output directory
OUTPUT_DIR="output"

# Create the output directory if it doesn't exist
mkdir -p $OUTPUT_DIR

# Run `go test` multiple times
for i in $(seq 1 $NUM_RUNS); do
    OUTPUT_FILE="$OUTPUT_DIR/output$i.txt"
    echo "Running go test... (Run $i)"
    go test > $OUTPUT_FILE 2>&1
    echo "Results stored in $OUTPUT_FILE"
done

echo "All tests completed. Results stored in the '$OUTPUT_DIR' directory."