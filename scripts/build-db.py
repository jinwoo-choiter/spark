import yaml
import sqlite3
import os
import argparse
from pathlib import Path
from typing import List, Union


def flatten_dict(d: dict, parent_key: str = "", sep: str = ".") -> dict:
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, str(v)))
    return dict(items)


def process_yaml_file(file_path: Path) -> dict:
    with open(file_path, "r") as f:
        return yaml.safe_load(f)


def process_directory(directory: Path) -> dict:
    result = {}
    for yaml_file in directory.glob("**/*.yaml"):
        data = process_yaml_file(yaml_file)
        # Use file name as prefix
        prefix = yaml_file.stem
        result[prefix] = data
    return result


def create_database(db_path: Path, data: dict):
    # Create database directory
    db_path.parent.mkdir(parents=True, exist_ok=True)

    # Connect to SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create parameters table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS parameters (
        key TEXT PRIMARY KEY,
        value TEXT
    )
    """)

    # Flatten data and insert into database
    flat_data = flatten_dict(data)
    for key, value in flat_data.items():
        cursor.execute(
            "INSERT OR REPLACE INTO parameters (key, value) VALUES (?, ?)", (key, value)
        )

    # Save changes and close connection
    conn.commit()
    conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="Convert yaml files to SQLite database"
    )
    parser.add_argument("sources", nargs="+", help="YAML file or directory")
    parser.add_argument(
        "-o", "--output", required=True, help="Output database file path"
    )
    args = parser.parse_args()

    # Process input data
    combined_data = {}
    for source in args.sources:
        source_path = Path(source)
        if source_path.is_file():
            if source_path.suffix.lower() == ".yaml":
                data = process_yaml_file(source_path)
                combined_data[source_path.stem] = data
        elif source_path.is_dir():
            dir_data = process_directory(source_path)
            combined_data.update(dir_data)
        else:
            print(f"Warning: {source} is not a valid file or directory.")

    # Create database
    create_database(Path(args.output), combined_data)
    print(f"Database created at {args.output}")


if __name__ == "__main__":
    main()
