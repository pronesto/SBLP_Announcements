import csv
import sys

# --- I/O Functions ---

def read_input_file(filename):
    """Reads CSV and returns a list of dictionaries."""
    try:
        with open(filename, mode='r', encoding='utf-8') as f:
            return list(csv.DictReader(f))
    except FileNotFoundError:
        print(f"Error: File '{filename}' not found.", file=sys.stderr)
        sys.exit(1)

def print_output_file(destination, data):
    """Writes the processed data to a file or stdout."""
    is_stdout = destination.lower() in ('-', 'stdout')
    if is_stdout:
        out_handle = sys.stdout
    else:
        out_handle = open(destination, mode='w', encoding='utf-8', newline='')
    fieldnames = ['Nome', 'FirstName', 'Surname', 'Email', 'Country']
    writer = csv.DictWriter(out_handle, fieldnames=fieldnames)
    try:
        writer.writeheader()
        writer.writerows(data)
    finally:
        if not is_stdout:
            out_handle.close()

# --- Transformation Functions ---

def remove_duplicates(data):
    """Filters out duplicate emails."""
    return list({row['Email']: row for row in data}.values())

def split_names(data):
    """Maps the name splitting logic over the dataset."""
    def transform(row):
        parts = row['Nome'].split(' ', 1)
        # Split the dictionary creation over multiple lines
        return {
            **row,
            'FirstName': parts[0],
            'Surname': (parts[1] if len(parts) > 1 else "")
        }
    return map(transform, data)

def find_country_names(data):
    """Maps the country derivation logic over the dataset."""
    def transform(row):
        tld = row['Email'].split('.')[-1]
        country = tld if len(tld) == 2 else "br"
        return {**row, 'Country': country}
    return map(transform, data)

# --- Main Function ---

def run_pipeline(input_file_name, output_file_name):
    reader = read_input_file(input_file_name)
    tmp0   = remove_duplicates(reader)
    tmp1   = split_names(tmp0)
    tmp2   = find_country_names(tmp1)
    print_output_file(output_file_name, tmp2)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        error_msg = "Usage: python pipeline.py <input.csv> <output_or_stdout>"
        print(error_msg, file=sys.stderr)
        sys.exit(1)
    else:
        run_pipeline(sys.argv[1], sys.argv[2])
