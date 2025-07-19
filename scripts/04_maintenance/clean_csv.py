import csv
import sys

input_file = sys.argv[1]
output_file = sys.argv[2]
year = sys.argv[3]

# Use Python's built-in CSV module with strict handling
with open(input_file, 'r', encoding='latin1') as infile, \
     open(output_file, 'w', encoding='utf8') as outfile:
    
    # Read header
    header = infile.readline().strip()
    outfile.write(header + ";year\n")
    
    # Process data with minimal processing (just read/write lines)
    for line in infile:
        # Simple approach: skip lines that don't have the right number of columns
        fields = line.strip().split(';')
        if len(fields) == 15:  # Expected number of columns
            outfile.write(line.strip() + f";{year}\n")
