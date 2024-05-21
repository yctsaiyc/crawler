import csv
import glob
from datetime import datetime


def process_csv(file_path):
    rows = []

    with open(file_path, newline="") as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            row = [cell.replace('"', "") for cell in row]
            if len(row) >= 15:  # Ensure the row has at least 15 columns
                cell = row[14]  # Column 15 is index 14 (0-based index)
                try:
                    datetime.strptime(cell, "%Y-%m-%d")
                except ValueError:
                    if cell != "":
                        print(cell)
                        row[14] = ""
            rows.append(row)

    with open(file_path, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(rows)


def process_csv_b(file_path):
    cleaned_lines = []

    with open(file_path, "rb") as csvfile:
        for idx, row in enumerate(csvfile):
            row = row.replace(b'"', b"")  # Replace all double quotes with empty strings
            columns = row.split(b",")
            cell = columns[14]
            try:
                datetime.strptime(cell.decode("utf-8"), "%Y-%m-%d")
            except (ValueError, TypeError):
                if cell != b"":
                    print(f"Invalid date in row {idx}: {cell}")
                    columns[14] = b""
            cleaned_lines.append(b",".join(columns))

    with open(file_path, "wb") as csvfile:
        csvfile.writelines(cleaned_lines)


dirty_files = ["agent.lvr_main_a_001.csv", "agent.lvr_main_a_002.csv"]
utf8_files = [file for file in glob.glob("./*.csv") if file not in dirty_files]

for file in dirty_files:
    print(file)
    process_csv_b(file)

# for file in utf8_files:
#     print(file)
#     process_csv(file)
