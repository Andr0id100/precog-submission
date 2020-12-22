from pymongo import MongoClient
import xml.etree.ElementTree as ET

file2collection = lambda x: x[len("stackoverflow.com/"):-len(".xml")].lower()

FILENAMES = [
    "stackoverflow.com/Users.xml",
    "stackoverflow.com/Posts.xml",
    "stackoverflow.com/Votes.xml",
    "stackoverflow.com/Tags.xml",
    "stackoverflow.com/Badges.xml",
]

client = MongoClient("localhost", 27017)
db = client["stackoverflow"]
print("\nConnected to Database")

for filename in FILENAMES:
    print()
    collection_name = file2collection(filename)

    if collection_name in db.list_collection_names():
        db[collection_name].drop()
        print("Dropped existing collection:", collection_name)
    collection = db[collection_name]
    
    context = ET.iterparse(filename, events=("start", ))
    _, root = next(context)

    counter = 0
    print("Inserting in collection:", collection_name)
    for event, elem in context:
        counter += 1
        collection.insert_one(elem.attrib)
        root.clear()
        
        # Printing this to know that the program is not stuck
        print("\r", counter, sep="", end="")

    print("\nDone")

print("Parsed all files")