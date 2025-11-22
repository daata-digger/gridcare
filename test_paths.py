import os
import sys
from pathlib import Path

print("Current working directory:", os.getcwd())
print("\nPython version:", sys.version)
print("\nChecking paths...")

paths_to_check = [
    "storage/bronze/grid/load",
    "storage/bronze/weather",
    "storage/silver",
]

for path_str in paths_to_check:
    p = Path(path_str)
    print(f"\n{path_str}:")
    print(f"  Exists: {p.exists()}")
    print(f"  Absolute: {p.absolute()}")
    if p.exists():
        print(f"  Contents: {list(p.iterdir())[:5]}")  # First 5 items