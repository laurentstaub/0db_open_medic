import sys

input_file = sys.argv[1]
output_file = sys.argv[2]
year = sys.argv[3]

# Use binary mode and explicit encoding handling
with open(input_file, 'rb') as infile:
    # Try to detect encoding - often Latin-1 or Windows-1252 for French data
    content = infile.read()
    try:
        text = content.decode('latin1')
    except UnicodeDecodeError:
        try:
            text = content.decode('cp1252')  # Windows encoding
        except UnicodeDecodeError:
            text = content.decode('utf-8', errors='replace')  # Fallback
    
    lines = text.split('\n')
    header = lines[0]
    
    with open(output_file, 'w', encoding='utf-8') as outfile:
        # Write header with year column
        outfile.write(header + ";year\n")
        
        # Process data rows
        for i in range(1, len(lines)):
            if lines[i].strip():  # Skip empty lines
                fields = lines[i].split(';')
                if len(fields) == 21:  # Expect 21 columns
                    outfile.write(lines[i] + f";{year}\n")
