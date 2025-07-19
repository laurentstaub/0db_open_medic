import pandas as pd
import sys

input_file = sys.argv[1]
output_file = sys.argv[2]
year = int(sys.argv[3])
chunk_size = 100000  # Process in chunks

# Create an empty output file
with open(output_file, 'w', encoding='utf-8') as f:
    f.write('')  # Initialize empty file

# Process the file in chunks
chunk_num = 0
for chunk in pd.read_csv(input_file, sep=';', chunksize=chunk_size, 
                         encoding='latin1', low_memory=False,
                         quotechar='"', escapechar='\\', 
                         on_bad_lines='warn'):
    
    # Add year column
    chunk['year'] = year
    
    # Sanitize text fields
    for col in chunk.columns:
        if chunk[col].dtype == 'object':
            chunk[col] = chunk[col].astype(str).str.replace('\n', ' ').str.replace('\r', ' ')
    
    # Write to CSV (append mode after first chunk)
    mode = 'a' if chunk_num > 0 else 'w'
    chunk.to_csv(output_file, sep=';', index=False, mode=mode, 
                header=(chunk_num==0), encoding='utf-8',
                quoting=1)  # QUOTE_ALL
    
    chunk_num += 1
    print(f"Processed chunk {chunk_num} ({chunk_size * chunk_num} rows)")

print(f"Processing complete. Output written to {output_file}")
