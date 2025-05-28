import random
import string

# Starting ID after the provided data
start_id = 1
num_rows = 1000
output_file = "inserts.sql"

# Generate random names (2-3 letters)
def random_name(length):
    return ''.join(random.choices(string.ascii_lowercase, k=length))

# Open file to write SQL statements
with open(output_file, 'w') as f:
    for i in range(start_id, start_id + num_rows):
        name = random_name(random.randint(2, 3))
        age = random.randint(18, 25)
        gpa = round(random.uniform(2.0, 4.0), 2)
        f.write(f"insert into t (id, name, age, gpa) values ({i}, '{name}', {age}, {gpa});\n")

print(f"Generated {num_rows} SQL INSERT statements and saved to {output_file}")