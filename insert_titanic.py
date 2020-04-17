import os
import pandas as pd 
import psycopg2
from tabulate import tabulate
from psycopg2.extras import execute_values
import re

# Connection details for the Postgres database
DB_PG_HOST = os.getenv("DB_PG_HOST", default="missing")
DB_PG_DBNAME = os.getenv("DB_PG_DBNAME", default="missing")
DB_PG_USER = os.getenv("DB_PG_USER", default="missing")
DB_PG_PASSWORD = os.getenv("DB_PG_PASSWORD", default="missing")

# Define a dataframe data transformation function
def trans_num_bool(val):
    if val == 1:
        return "True"
    
    return "False"
    
def trans_pclass(val):
    map = {
        1: "First",
        2: "Second",
        3: "Third"
    }

    return map.get(val, "MISSING")

def trans_sex(val):
    if val == "male":
        return val 
    if val == "female":
        return val
    return "MISSING"

def trans_py_type(val):
    return val.item()

def transform_df(df_in):
    df_out = df_in.copy()

    df_out["Survived"] = df_out["Survived"].apply(trans_num_bool)
    df_out["Pclass"] = df_out["Pclass"].apply(trans_pclass)
    df_out["Sex"] = df_out["Sex"].apply(trans_sex)
    df_out["Siblings/Spouses Aboard"] = df_out["Siblings/Spouses Aboard"].astype(float)
    df_out["Parents/Children Aboard"] = df_out["Parents/Children Aboard"].astype(float)

    return df_out

# Read in the titanic csv dataset
print(f'\n**************\nINFO: Starting processing...')
print(f'INFO: Importing data from "titanic.csv"')
df_csv = pd.read_csv("./titanic.csv")

# Transform the dataframe
df_trn = transform_df(df_csv)

# Connect to the Postgres database
print(f'INFO: Connecting to the Postgres database...')
conn_pg = psycopg2.connect(
    dbname=DB_PG_DBNAME, 
    user=DB_PG_USER, 
    password=DB_PG_PASSWORD, 
    host=DB_PG_HOST)

# Test that have connected to the database
query_pg = f'SELECT 1'
csr_pg = conn_pg.cursor()
csr_pg.execute(query_pg)
rslts_pg = csr_pg.fetchall()

# Test the Postgres connection
if rslts_pg[0][0] == 1:
    print(f'INFO: You have connected successfully to server: {DB_PG_HOST} database: {DB_PG_DBNAME}')
else:
    print(f'ERROR: A connection error occurred\nExiting...')
    quit()

# SQL used to create a titanic table in Postgres
PG_CREATE_TABLE = """
CREATE TABLE titanic_data (
	titanic_id              serial NOT NULL,
	survived                bool NOT NULL,
	passenger_class         text NOT NULL,
	"name"                  text NOT NULL,
	sex                     varchar(10) NOT NULL,
	age                     numeric, 
	siblings_spouse_aboard  numeric,
	parents_children_aboard numeric,
	fare                    money,
	PRIMARY KEY             (titanic_id)
);
"""

# Drop the intended table if it exists
print(f'INFO: Drop and create a new Postgres db table...')
csr_pg.execute("DROP TABLE IF EXISTS titanic_data")
conn_pg.commit()      # commit the create transaction

# Create the new Postgres table
csr_pg.execute(PG_CREATE_TABLE)
conn_pg.commit()      # commit the create transaction

# execute_values insert statement
PG_INSERT = """
INSERT INTO titanic_data 
  (survived, passenger_class, name, sex, age, siblings_spouse_aboard, parents_children_aboard, fare) 
  VALUES %s;
"""

# Convert the dataframe to a list of tuples
records = df_trn.to_records(index=False)
rows = list(records)

# Insert the rows exported from the sqlite database into the Postgres database table 
print(f'INFO: Insert the transformed titanic.csv data into the Postgres db...')
execute_values(
    csr_pg, 
    PG_INSERT, 
    rows)
conn_pg.commit() 

# Count the number of rows in the the newly imported table
csr_pg.execute("SELECT count(*) FROM titanic_data")
rslts_pg = csr_pg.fetchone()

print(f'INFO: The number of rows imported into Postgres (expecting: {len(df_trn.index)}) is: {rslts_pg[0]}')

# Question #1: How many passengers survived, and how many died?
PG_Q01A = "SELECT count(*) FROM titanic_data WHERE survived = false"
csr_pg.execute(PG_Q01A)
rslts_pg = csr_pg.fetchone()
print(f'\nOUT:  How many passengers did not survive? {rslts_pg[0]}')

PG_Q01B = "SELECT count(*) FROM titanic_data WHERE survived = true"
csr_pg.execute(PG_Q01B)
rslts_pg = csr_pg.fetchone()
print(f'OUT:  How many passengers survived? {rslts_pg[0]}')

# Question #2: How many passengers were in each class?
PG_Q02 = "SELECT passenger_class, count(*) from titanic_data GROUP BY passenger_class ORDER BY count DESC"
csr_pg.execute(PG_Q02)
rslts_pg = csr_pg.fetchall()
print(f'\nOUT:  How many passengers were in each class?')
print(tabulate(rslts_pg))

# Question #3: How many passengers survived/died within each class?
PG_Q03 = """
SELECT passenger_class, survived, count(*) 
FROM titanic_data 
GROUP BY passenger_class, survived 
ORDER BY passenger_class ASC, survived ASC
"""
csr_pg.execute(PG_Q03)
rslts_pg = csr_pg.fetchall()
print(f'\nOUT:  How many passengers survived/died within each class?')
print(tabulate(rslts_pg, headers=["Class","Survived?", "Count"]))

# Question #4: What was the average age of survivors vs nonsurvivors?
PG_Q04 = """
SELECT survived, ROUND(AVG(age), 2) 
AS average_age 
FROM titanic_data 
GROUP BY survived 
ORDER BY average_age 
"""
csr_pg.execute(PG_Q04)
rslts_pg = csr_pg.fetchall()
print(f'\nOUT:  What was the average age of survivors vs nonsurvivors?')
print(tabulate(rslts_pg, headers=["Survived?", "Average Age"]))

# Question #5: What was the average age of each passenger class?
PG_Q05 = """
SELECT passenger_class, ROUND(AVG(age), 2) 
AS average_age 
FROM titanic_data 
GROUP BY passenger_class 
ORDER BY average_age DESC
"""
csr_pg.execute(PG_Q05)
rslts_pg = csr_pg.fetchall()
print(f'\nOUT:  What was the average age of each passenger class?')
print(tabulate(rslts_pg, headers=["Passenger Class", "Average Age"]))

# Question #6: What was the average fare by passenger class? By survival?
PG_Q06A = """
SELECT passenger_class, ROUND(AVG(fare::numeric), 2) 
AS average_fare 
FROM titanic_data 
GROUP BY passenger_class 
ORDER BY average_fare DESC
"""
csr_pg.execute(PG_Q06A)
rslts_pg = csr_pg.fetchall()
print(f'\nOUT:  What was the average fare by passenger class?')
print(tabulate(rslts_pg, headers=["Passenger Class", "Average Fare"]))

PG_Q06B = """
SELECT survived, ROUND(AVG(fare::numeric), 2) 
AS average_fare 
FROM titanic_data 
GROUP BY survived 
ORDER BY average_fare DESC
"""
csr_pg.execute(PG_Q06B)
rslts_pg = csr_pg.fetchall()
print(f'OUT:  What was the average fare by passenger class?')
print(tabulate(rslts_pg, headers=["Survived?", "Average Fare"]))

# Question #7: How many siblings/spouses aboard on average, by passenger class? By survival?
PG_Q07A = """
SELECT passenger_class, ROUND(AVG(siblings_spouse_aboard), 2) 
AS avg_sib_spse FROM titanic_data 
GROUP BY passenger_class 
ORDER BY avg_sib_spse DESC
"""
csr_pg.execute(PG_Q07A)
rslts_pg = csr_pg.fetchall()
print(f'\nOUT:  How many siblings/spouses aboard on average, by passenger class?')
print(tabulate(rslts_pg, headers=["Passenger Class", "Average # of Siblings and Spouse"]))

PG_Q07B = """
SELECT survived, ROUND(AVG(siblings_spouse_aboard), 2) 
AS avg_sib_spse FROM titanic_data 
GROUP BY survived 
ORDER BY avg_sib_spse DESC
"""
csr_pg.execute(PG_Q07B)
rslts_pg = csr_pg.fetchall()
print(f'OUT:  How many siblings/spouses aboard on average, by passenger survival?')
print(tabulate(rslts_pg, headers=["Survived?", "Average # of Siblings and Spouse"]))

# Question #8: How many parents/children aboard on average, by passenger class? By survival?
PG_Q08A = """
SELECT passenger_class, ROUND(AVG(parents_children_aboard), 2) 
AS avg_par_chd FROM titanic_data 
GROUP BY passenger_class 
ORDER BY avg_par_chd DESC
"""
csr_pg.execute(PG_Q07A)
rslts_pg = csr_pg.fetchall()
print(f'\nOUT:  How many parents/children aboard on average, by passenger class?')
print(tabulate(rslts_pg, headers=["Passenger Class", "Average # of Children & Parents"]))

PG_Q08B = """
SELECT survived, ROUND(AVG(parents_children_aboard), 2) 
AS avg_par_chd FROM titanic_data 
GROUP BY survived 
ORDER BY avg_par_chd DESC
"""
csr_pg.execute(PG_Q08B)
rslts_pg = csr_pg.fetchall()
print(f'OUT:  How many parents/children aboard on average, by passenger class?')
print(tabulate(rslts_pg, headers=["Survived?", "Average # of Children & Parents"]))

# Question #9: Do any passengers have the same name?
PG_Q09 = "SELECT name FROM titanic_data"
csr_pg.execute(PG_Q09)
rslts_pg = csr_pg.fetchall()

lst_name = []
for itm in rslts_pg:
    tmp_str = re.sub("^\w+\.{1}\s+", "", itm[0])
    tmp_str = re.sub("\([\w\s]+\)", "", tmp_str)
    tmp_str = re.sub("\s\s+", " ", tmp_str)
    tmp_str = tmp_str.strip()
    lst_name.append(tmp_str)

map_dupe = {}
for itm in lst_name[:75]:
    if itm in map_dupe:
        map_dupe[itm] = map_dupe[itm] + 1
    else:
        map_dupe[itm] = 1

ctr = 0
for key in map_dupe:
    if map_dupe[key] > 1:
        ctr = ctr + 1
    
print(f'\nOUT:  Do any passengers have the same name?\n')
print(f'OUT:  There are {ctr} duplicate names\n')

# Clean up database connections
print(f'\nINFO: Cleaning up database connections...')
csr_pg.close()
conn_pg.close()

print(f'INFO: Processing complete\n**************\n')