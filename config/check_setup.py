import sys
from importlib.metadata import version

print("Python", sys.version.split()[0])
for pkg in ["psycopg2-binary", "azure-eventhub", "python-dotenv", "jupyter", "pandas"]:
    print(f"{pkg:18} {version(pkg)}")

import psycopg2
import azure.eventhub
import dotenv
print("All imports OK")