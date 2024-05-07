import os

# imports for relational db
from sqlite3 import connect
import json
from json import load

import sqlite3

# imports for graph db
from rdflib import RDF, Graph, URIRef, Literal
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
import SPARQLWrapper

# imports for both
import pandas as pd
from pandas import DataFrame, Series, concat, read_csv, read_sql

from pprint import pprint as pp


# handlers # Hubert

class Handler:
    def __init__(self):
        self.dbPathOrUrl = ""

    def getDbPathOrUrl(self):
        return self.dbPathOrUrl

    def setDbPathOrUrl(self, pathOrUrl: str) -> bool:
        self.dbPathOrUrl = pathOrUrl
        return self.dbPathOrUrl == pathOrUrl

class UploadHandler(Handler):
    def __init__(self):
        super().__init__()

    def pushDataToDb(self):
        pass


# Class to upload data from JSONs to SQLite database #Lucrezia

class ProcessDataUploadHandler(Handler):
    def __init__(self, db_name="relational_database.db"):
        super().__init__()
        self.db_name = db_name

    def process_data(self, json_data):
        # Create a dictionary to map object IDs to internal identifiers
        object_id_mapping = {}
        for item in json_data:
            object_id = item['object id']
            object_internal_id = f"CH Object-{object_id}"
            object_id_mapping[object_id] = object_internal_id

        # Create DataFrames for each activity type and tools
        activity_dfs = {}  # a dictionary
        tools_data = []  # a list of dictionaries
        for activity_type in json_data[0].keys():
            if activity_type != 'object id':
                activity_data = []
                activity_count = 1
                for item in json_data:
                    activity = item.get(activity_type, {})
                    if activity:  # Check if activity data exists
                        object_id = item['object id']
                        activity_internal_id = f"{activity_type.capitalize()}-{activity_count:02d}"  # Generate unique identifier
                        activity_count += 1
                        activity_data.append({
                            "Activity_internal_id": activity_internal_id,
                            "Refers To": object_id_mapping.get(object_id, ""),
                            "Responsible Institute": activity.get("responsible institute", ""),
                            "Responsible Person": activity.get("responsible person", ""),
                            "Tool_internal_id": f"{activity_internal_id}-tool",  # Link to tools DataFrame
                            "Start Date": activity.get("start date", ""),
                            "End Date": activity.get("end date", ""),
                            "Technique": activity.get("technique", "") if activity_type == "acquisition" else "",
                        })
                        # Collect tools data
                        if 'tool' in activity:
                            for tool in activity['tool']:
                                tools_data.append({
                                    "Tool_internal_id": f"{activity_internal_id}-tool",  # Generate unique tool internal identifier
                                    "Tool": tool,
                                    "Activity_internal_id": activity_internal_id
                                })
                activity_dfs[activity_type] = pd.DataFrame(activity_data)

        # Create DataFrame for tools
        tools_df = pd.DataFrame(tools_data)

        return activity_dfs, tools_df

    def pushDataToDb(self, activity_dfs, tools_df):
        # Connect to the SQLite database
        conn = sqlite3.connect(self.db_name)

        # Loop through each DataFrame and upload it to the database
        for activity_type, df in activity_dfs.items():
            df.to_sql(activity_type.lower(), conn, if_exists='replace', index=False)

        tools_df.to_sql('tools', conn, if_exists='replace', index=False)

        # Close the connection
        conn.close()

# Load JSON data
file_path = os.path.join("resources", "process.json")
with open(file_path, "r") as file:
    json_data = json.load(file)

# Instantiate the ProcessDataUploadHandler
process_data_upload_handler = ProcessDataUploadHandler()

# Process the data
activity_dfs, tools_df = process_data_upload_handler.process_data(json_data)

# Push the data to the database
process_data_upload_handler.pushDataToDb(activity_dfs, tools_df)





# Here can go the class for uploading CSV files to Blazegraph database (?)

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