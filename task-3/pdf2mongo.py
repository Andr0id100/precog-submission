import sys
import os
import tabula
from pymongo import MongoClient

null = None

FILENAME = sys.argv[1]

client = MongoClient("localhost", 27017)
print("Connected to Database")
db = client["pdf-tables"]

tables = tabula.read_pdf(FILENAME, pages="all")

for i in range(len(tables)):
    collection_name = os.path.basename(FILENAME)[:-4] + "-" + str(i+1)
    if collection_name in db.list_collection_names():
        db[collection_name].drop()
    print("Inserting collection", collection_name)

    collection = db[collection_name]
    table = tables[i]

    json = eval(table.to_json())
    columns = list(json.keys())

    row_count = len(json[columns[0]])
    for i in range(row_count):
        document = {}
        for col in columns:
            document[col] = json[col][str(i)]
    collection.insert_one(document)
        
print("Done")