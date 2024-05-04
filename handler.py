# imports for relational db
from sqlite3 import connect
from json import load

import sqlite3

# imports for graph db
from rdflib import RDF, Graph, URIRef, Literal
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
import SPARQLWrapper

# imports for both
import pandas as pd
from pandas import DataFrame, concat, read_csv, read_sql, Series

from pprint import pprint as pp


# handlers # Hubert

class Handler:
    def __init__(self):
        self.dbPathOrUrl = ""

    def __init__(self, dbPathOrUrl): # overrides the first init but this class itself is never created so oh well
        self.dbPathOrUrl = dbPathOrUrl

    def getDbPathOrUrl(self):
        return self.dbPathOrUrl

    def setDbPathOrUrl(self, pathOrUrl):
        self.dbPathOrUrl = pathOrUrl
        if self.dbPathOrUrl == pathOrUrl:
            return True
        return False


class UploadHandler(Handler):
    def __init__(self):
        self.dbPathOrUrl = ""

    def __init__(self, dbPathOrUrl): # overrides the first init but this class itself is never created so oh well
        self.dbPathOrUrl = dbPathOrUrl

    def pushDataToDb(path): #shouldn't be specified 'self' as a parameter?
        return False
    
    # alternative method: def pushDataToDb(self, json_file: str) -> bool:


# Class to upload data from JSONs to SQLite database #Lucrezia

class ProcessDataUploadHandler(UploadHandler):
    def __init__(self, json_file_path, db_file_path):
        super().__init__()  # Initialize parent class
        self.json_file_path = json_file_path
        self.db_file_path = db_file_path

    def process_data(self):
        # Read JSON file into a pandas DataFrame and avoid NaN objects
        df = pd.read_json(self.json_file_path, keep_default_na=False)

        # Connect to SQLite database
        conn = sqlite3.connect(self.db_file_path)

        # Check if table exists in the database
        table_exists = self.check_table_exists(conn)

        # Write DataFrame to SQLite database
        if table_exists:
            # Append data to existing table, avoiding duplicates
            self.append_data_to_table(df, conn)
        else:
            # If table does not exist, create new table
            df.to_sql('activities_data_table', conn, if_exists='replace', index=False, dtype='string')

        # Commit changes and close connection
        conn.commit()
        conn.close()

    # Check if 'activities_data_table' exists in the database.
    def check_table_exists(self, conn): 
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='activities_data_table'")
        return cursor.fetchone() is not None

    #Append new data to 'activities_data_table', avoiding duplicates
    def append_data_to_table(self, df, conn):
        existing_data = pd.read_sql('SELECT * FROM activities_data_table', conn) # Retrieve existing data from database

        # Filter new data to exclude existing duplicates and replace NaN with empty string
        new_data = df[~df.isin(existing_data)].fillna('')

        # Append filtered new data to 'activities_data_table'
        new_data.to_sql('activities_data_table', conn, if_exists='append', index=False)


#      Example usage:
# json_file_path = 'data.json'
# db_file_path = 'data.db'
# handler = ProcessDataUploadHandler(json_file_path, db_file_path)
# handler.process_data()


# Here can go the class for uploading CSV files to Blazegraph database?
class MetadataUploadHandler(UploadHandler):
    def pushDataToDb(self, path):
        pass



class QueryHandler(Handler): 
    def __init__(self):
        self.dbPathOrUrl = ""

    def __init__(self, dbPathOrUrl): # overrides the first init but this class itself is never created so oh well
        self.dbPathOrUrl = dbPathOrUrl

    def getById(self, id): # -> DataFrame:
        return None # never accessed here and overridden in child class



# Class to interact with SQL database running queries

class ProcessDataQueryHandler(QueryHandler): #Lucrezia
    def __init__(self, dbPathOrUrl):
        super().__init__()
        self.dbPathOrUrl = dbPathOrUrl

    # helper method to reduce code clutter # Hubert!!
    def executeQuery(self, sql_command): 
        connection = connect(self.dbPathOrUrl)
        cursor = connection.cursor()
        cursor.execute(sql_command)
        df = pd.DataFrame(cursor.fetchall(), columns=[description[0] for description in cursor.description])
        connection.close()
        return df

    def getAllActivities(self) -> DataFrame:
        sql_command = """
        SELECT *
        FROM Activity
        """
        return self.executeQuery(sql_command)
    
    def getActivitiesByResponsibleInstitution(self, partialName: str) -> DataFrame:
        sql_command = f"""
        SELECT *
        FROM Activity
        WHERE responsibleInstitute LIKE '%{partialName}%'
        """
        return self.executeQuery(sql_command)
    
    def getActivitiesByResponsiblePerson(self, partialName: str) -> DataFrame:
        sql_command = f"""
        SELECT *
        FROM Activity
        WHERE responsiblePerson LIKE '%{partialName}%'
        """
        return self.executeQuery(sql_command)
    
    def getActivitiesUsingTool(self, partialName: str) -> DataFrame:
        sql_command = f"""
        SELECT *
        FROM Activity
        WHERE tool LIKE '%{partialName}%'
        """
        return self.executeQuery(sql_command)
    
    def getActivitiesStartedAfter(self, date: str) -> DataFrame:
        sql_command = f"""
        SELECT *
        FROM Activity
        WHERE startDate > '{date}'
        """
        return self.executeQuery(sql_command)
    
    def getActivitiesEndedBefore(self, date: str) -> DataFrame:
        sql_command = f"""
        SELECT *
        FROM Activity
        WHERE endDate < '{date}'
        """
        return self.executeQuery(sql_command)

    def getAcquisitionsByTechnique(self, partialName: str) -> DataFrame:
        sql_command = """
        SELECT *
        FROM Acquisition
        WHERE LOWER(technique) LIKE ?
        ORDER BY objectId
        """
        # Using parameterized query to prevent SQL injection
        partialName = '%' + partialName.lower() + '%'  # Convert partialName to lowercase
        return self.executeQuery(sql_command, params=(partialName,))
 

        


class MetadataQueryHandler(QueryHandler):
    def __init__(self):
        self.dbPathOrUrl = ""

    # helper method to reduce code clutter
    def execute_query(self, query): 
        sparql = SPARQLWrapper.SPARQLWrapper(self.dbPathOrUrl);
        sparql.setReturnFormat(SPARQLWrapper.JSON);
        sparql.setQuery(query);
        try:
            ret = sparql.queryAndConvert();
        except Exception as e:
            print(e);
        
        # create and fill a dataframe with the results
        df_columns = ret["head"]["vars"];
        df = pd.DataFrame(columns = df_columns);
        for row in ret["results"]["bindings"]:
            row_dict = {}
            for column in df_columns:
                if column in row:
                    row_dict.update({column: row[column]["value"]});
            df.loc[len(df)] = row_dict;
            df = df.reset_index(drop=True);
        return df;

    # UML methods go here