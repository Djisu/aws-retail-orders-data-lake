from pathlib import Path
import shutil

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SOURCE_DIR = PROJECT_ROOT / "data" / "sample_input"
RAW_DIR = PROJECT_ROOT / "data" / "raw"

def ingest_files():
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    csv_files = sorted(SOURCE_DIR.glob("*.csv"))

    if not csv_files:
        print("No CSV files found in sample_input.")
        return

    for file_path in csv_files:
        destination = RAW_DIR / file_path.name
        shutil.copy2(file_path, destination)
        print(f"Ingested: {file_path.name} -> {destination}")

    print(f"\nIngestion complete. {len(csv_files)} file(s) copied to raw zone.")

if __name__ == "__main__":
    ingest_files()
