import os
import sqlite3
from prettytable import PrettyTable
from tabulate import tabulate
import pymongo
import pandas as pd

# ******************************************************
# IMPORT RPG DATA INTO MONGODB
# ******************************************************

# Connection details for the sqlite3 database
DB_FILEPATH = "./rpg_db.sqlite3"
DB_SLT_TABLES = [
    "charactercreator_character", 
    "charactercreator_character_inventory",
    "armory_item",
    "armory_weapon",
    "charactercreator_cleric",
    "charactercreator_fighter",
    "charactercreator_mage",
    "charactercreator_necromancer",
    "charactercreator_thief"]

# Connect to the sqlite3 database
print(f'\n**************\nINFO: Processing starting...')
conn_sl = sqlite3.connect(DB_FILEPATH)
conn_sl.row_factory = sqlite3.Row
cur_sl = conn_sl.cursor()

# Test that the script has connected to the sqlite3 database
print(f'INFO: Test the sqlite3 db connection...')
rslts_sl = conn_sl.execute("SELECT 1").fetchall()
# Test the sqlite connection
if rslts_sl[0][0] == 1:
    print(f'INFO: You have connected successfully to {DB_FILEPATH}')
else:
    print(f'ERROR: A connection error occurred. Exiting...')
    quit()

# Connection details for the MongoDB database
DB_MG_DMN = os.getenv("DB_MG_DMN", default="missing")
DB_MG_DB  = os.getenv("DB_MG_DB",  default="missing")
DB_MG_USR = os.getenv("DB_MG_USR", default="missing")
DB_MG_PWD = os.getenv("DB_MG_PWD", default="missing")
mg_conn_string = f'mongodb://{DB_MG_USR}:{DB_MG_USR}@{DB_MG_DMN}/{DB_MG_DB}?retryWrites=false'

# Connect to the MongoDB database
clnt_mg = pymongo.MongoClient(mg_conn_string)
db_mg   = clnt_mg[DB_MG_DB]

# Define a map of virtual "tables" and table row counts (stored in memory)
map_tables       = {}
map_tables_count = {}

# Iterate through the sqlite3 tables and import the table contents in to memory
for sqlt_tabl in DB_SLT_TABLES:
    # Define a temp array to house a sqlite db's rows
    arr_tmp = []

    # Query rows from the sqlite table being iterated upon
    query_sl = f'SELECT * FROM {sqlt_tabl}'
    rslts_sl = cur_sl.execute(query_sl).fetchall()

    # Iterate through each query (of table rows)
    for row in rslts_sl:
        # Append the table row to the temp array (as a dict)
        arr_tmp.append(dict(row))

    # Store the temp array in our "virtual" set of tables
    map_tables[sqlt_tabl] = arr_tmp

# Iterate through our set of virtual tables
print(f'INFO: Insert the sqlite table data into a collection of the same name')
for vtbl in map_tables.keys():
    # Set the current MongoDB collection
    col_mg = db_mg[vtbl]
    # Drop the collection if it exists
    col_mg.drop()

    # Insert the rows from the virtual table for the table being iterated upon
    col_mg.insert_many(map_tables[vtbl])

    # Save a count of the newly inserted documents
    map_tables_count[vtbl] = col_mg.count_documents({})

for coll in map_tables_count:
    print(f'INFO: Collection {coll} has {map_tables_count[coll]} documents')

# Close db connections
print(f'INFO: Close the SQLite database connections')
cur_sl.close()
conn_sl.close()

# Question #1: How many total Characters are there?
count_mg = db_mg.charactercreator_character.distinct("name")
print(f'-----\nQuestion #1 How many total Characters are there? {len(count_mg)}\n\n')

# Question #2 How many of each specific subclass?
cols = ("cleric", "fighter", "mage", "necromancer", "thief")

count_cleric = len(db_mg.charactercreator_cleric.distinct("character_ptr_id"))
count_fighter = len(db_mg.charactercreator_fighter.distinct("character_ptr_id"))
count_mage = len(db_mg.charactercreator_mage.distinct("character_ptr_id"))
count_necromancer = len(db_mg.charactercreator_necromancer.distinct("mage_ptr_id"))
count_thief = len(db_mg.charactercreator_thief.distinct("character_ptr_id"))

t = PrettyTable(cols)
t.add_row((count_cleric, count_fighter, count_mage, count_necromancer, count_thief))

print(f'-----\nQuestion #2 How many of each specific subclass?')
print(t)

# Question #3 How many total Items?
cnt_total_items = len(db_mg.armory_item.distinct("item_id"))
print(f'\n-----\nQuestion #3 How many total Items? {cnt_total_items}')

# Question #4 How many of the Items are weapons?
cnt_total_weapons = len(db_mg.armory_weapon.distinct("item_ptr_id"))
print(f'\n-----\nQuestion #4 How many of the Items are weapons? {cnt_total_weapons}')

# Question #4 How many are not?
print(f'Question #4 How many are not? {cnt_total_items - cnt_total_weapons}\n\n')

# Question #5 How many Items does each character have? (Return first 20 rows)
pipeline = [
    {"$group": {"_id": "$character_id", "count": {"$sum": 1}}}
]

# Aggregate characters by the number of items they possess
lst_char_inv = list(db_mg.charactercreator_character_inventory.aggregate(pipeline))

lst_char_inv_sorted = sorted(lst_char_inv, key = lambda i: i['count'], reverse=True)

print(f'\nQuestion #5 How many Items does each character have? (Return first 20 rows)\n)')
print(tabulate(lst_char_inv_sorted[:20], headers="keys"))

# Question #6 How many Weapons does each character have? (Return first 20 rows)
pipeline = [
    {"$group": {"_id": "$item_ptr_id", "count": {"$sum": 1}}}
]
lst_char_inv = list(db_mg.armory_weapon.aggregate(pipeline))

lst_char_inv_sorted = sorted(lst_char_inv, key = lambda i: i['count'], reverse=True)

print(f'\nQuestion #6 How many Weapons does each character have? (Return first 20 rows\n)')
print(tabulate(lst_char_inv_sorted[:20], headers="keys"))

# Question #7 On average, how many Items does each Character have?
pipeline = [
    {"$group": {"_id": "$character_id", "average": {"$avg": 1}}}
]

# Aggregate characters by the number of items they possess
lst_char_inv = list(db_mg.charactercreator_character_inventory.aggregate(pipeline))
lst_char_inv_sorted = sorted(lst_char_inv, key = lambda i: i['_id'], reverse=True)

print(f'\nQuestion #7 On average, how many Items does each Character have?\n)')
print(tabulate(lst_char_inv, headers="keys"))

# Question #8 On average, how many Weapons does each character have?

num_items = db_mg.charactercreator_character_inventory.count_documents({})
num_chars = len(db_mg.charactercreator_character.distinct("character_id"))
print(f'\nQuestion #8 On average, how many Weapons does each character have?\nAverage number of items per character: {round((num_items/num_chars), 2)}')

clnt_mg.close()
exit()
