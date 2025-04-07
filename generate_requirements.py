# generate_requirements.py
import json

with open("Pipfile.lock") as f:
    lock = json.load(f)

packages = lock.get("default", {})
for name, meta in packages.items():
    version = meta.get("version", "")
    if version:
        print(f"{name}{version}")
