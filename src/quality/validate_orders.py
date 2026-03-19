from pathlib import Path
import csv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SAMPLE_INPUT_DIR = PROJECT_ROOT / "data" / "sample_input"

def validate_file(file_path):
    total_rows = 0
    invalid_quantity = 0
    invalid_unit_price = 0
    duplicate_order_ids = 0
    seen_order_ids = set()

    with open(file_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)

        for row in reader:
            total_rows += 1

            order_id = row["order_id"]
            quantity = int(row["quantity"])
            unit_price = int(row["unit_price"])

            if order_id in seen_order_ids:
                duplicate_order_ids += 1
            else:
                seen_order_ids.add(order_id)

            if quantity <= 0:
                invalid_quantity += 1

            if unit_price <= 0:
                invalid_unit_price += 1

    return {
        "file_name": file_path.name,
        "total_rows": total_rows,
        "duplicate_order_ids": duplicate_order_ids,
        "invalid_quantity": invalid_quantity,
        "invalid_unit_price": invalid_unit_price,
    }

def main():
    csv_files = sorted(SAMPLE_INPUT_DIR.glob("*.csv"))

    if not csv_files:
        print("No sample input files found.")
        return

    print("=== Validation Report ===\n")

    for file_path in csv_files:
        result = validate_file(file_path)
        print(f"File: {result['file_name']}")
        print(f"  Total rows           : {result['total_rows']}")
        print(f"  Duplicate order IDs  : {result['duplicate_order_ids']}")
        print(f"  Invalid quantity rows: {result['invalid_quantity']}")
        print(f"  Invalid unit price   : {result['invalid_unit_price']}")
        print()

if __name__ == "__main__":
    main()
