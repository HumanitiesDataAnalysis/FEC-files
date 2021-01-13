from pathlib import Path
from fecfile.dataframe import Fecfile, DateCollection
import sys

files = sys.argv[1:]
for file in files:
    print(file)
    d = DateCollection(file)
    d.cache_forms()