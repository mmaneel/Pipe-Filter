import re

# Read the data from the existing file with utf-8 encoding
with open("donnees_apres_etape_1_FiltreValidation.csv", "r", encoding='utf-8') as file:
    data = file.read()

# Define the split markers
split_1 = "donnees_apres_etape_2_FiltreNormalisation.csv"
split_2 = "donnees_apres_etape_3_FiltreTransformation.csv"

# Use regular expressions to split the content accurately and remove markers
pattern = f"({split_1}|{split_2})"
parts = re.split(pattern, data)

# Initialize parts
part_1 = part_2 = part_3 = ""

# Extract parts based on available markers
if len(parts) == 1:
    part_1 = parts[0].strip()
elif len(parts) == 3:
    part_1 = parts[0].strip()
    part_2 = parts[2].strip()
elif len(parts) == 5:
    part_1 = parts[0].strip()
    part_2 = parts[2].strip()
    part_3 = parts[4].strip()

# Write the parts to their respective files
if part_1:
    with open("donnees_apres_etape_1_FiltreValidation.csv", "w", encoding='utf-8') as file_1:
        file_1.write(part_1)

if part_2:
    with open("donnees_apres_etape_2_FiltreNormalisation.csv", "w", encoding='utf-8') as file_2:
        file_2.write(part_2)

if part_3:
    with open("donnees_apres_etape_3_FiltreTransformation.csv", "w", encoding='utf-8') as file_3:
        file_3.write(part_3)

print("Files created and data written successfully.")
