from impl import *

import os
from typing import Optional

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
from pandas import DataFrame, concat, read_csv, read_sql, Series




# Basic handlers 
# Hubert

class Handler:
    def __init__(self):
        self.dbPathOrUrl = ""

    def getDbPathOrUrl(self):
        return self.dbPathOrUrl

    def setDbPathOrUrl(self, pathOrUrl: str) -> bool:
        self.dbPathOrUrl = pathOrUrl
        return self.dbPathOrUrl == pathOrUrl
    
class QueryHandler(Handler): 
    def __init__(self, dbPathOrUrl=""):  # Provide a default value for dbPathOrUrl
        super().__init__()
        self.dbPathOrUrl = dbPathOrUrl

    def getById(self, id):
        return None



# Class to interact with SQL database 

class ProcessDataQueryHandler(Handler):
    def __init__(self, dbPathOrUrl=""):
        super().__init__()
        self.dbPathOrUrl = dbPathOrUrl
        self.db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), self.dbPathOrUrl))
    
    def executeQuery(self, sql_command):
        with connect(self.dbPathOrUrl) as conn:
            cursor = conn.cursor()
            cursor.execute(sql_command)
            columns = [description[0] for description in cursor.description]
            dataframe_query = pd.DataFrame(cursor.fetchall(), columns=columns)
        return dataframe_query

    
    def getAllActivities(self):
        sql_command = """
        SELECT Activity_internal_id, [Refers To], [Responsible Institute], [Responsible Person], Technique, [Start Date], [End Date]
        FROM Acquisition 
        UNION ALL
        SELECT Activity_internal_id, [Refers To], [Responsible Institute], [Responsible Person], Technique, [Start Date], [End Date]
        FROM Exporting 
        UNION ALL
        SELECT Activity_internal_id, [Refers To], [Responsible Institute], [Responsible Person], Technique, [Start Date], [End Date]
        FROM Modelling 
        UNION ALL
        SELECT Activity_internal_id, [Refers To], [Responsible Institute], [Responsible Person], Technique, [Start Date], [End Date]
        FROM Optimising 
        UNION ALL
        SELECT Activity_internal_id, [Refers To], [Responsible Institute], [Responsible Person], Technique, [Start Date], [End Date]
        FROM Processing
        """
        return self.executeQuery(sql_command)
    
    def getActivitiesByResponsibleInstitution(self, partialName):
        sql_command = f"""
        SELECT Activity_internal_id, [Refers To], [Responsible Institute], [Responsible Person], Technique, [Start Date], [End Date]
        FROM Acquisition WHERE [Responsible Institute] LIKE '%{partialName}%'
        UNION ALL
        SELECT Activity_internal_id, [Refers To], [Responsible Institute], [Responsible Person], Technique, [Start Date], [End Date]
        FROM Exporting WHERE [Responsible Institute] LIKE '%{partialName}%'
        UNION ALL
        SELECT Activity_internal_id, [Refers To], [Responsible Institute], [Responsible Person], Technique, [Start Date], [End Date]
        FROM Modelling WHERE [Responsible Institute] LIKE '%{partialName}%'
        UNION ALL
        SELECT Activity_internal_id, [Refers To], [Responsible Institute], [Responsible Person], Technique, [Start Date], [End Date]
        FROM Optimising WHERE [Responsible Institute] LIKE '%{partialName}%'
        UNION ALL
        SELECT Activity_internal_id, [Refers To], [Responsible Institute], [Responsible Person], Technique, [Start Date], [End Date]
        FROM Processing WHERE [Responsible Institute] LIKE '%{partialName}%'
        """
        return self.executeQuery(sql_command)
    
    def getActivitiesByResponsiblePerson(self, partialName: str) -> DataFrame:
        sql_command = f"""
        SELECT Activity_internal_id, [Refers To], [Responsible Institute], [Responsible Person], Technique, [Start Date], [End Date]
        FROM Acquisition
        WHERE [Responsible Person] LIKE '%{partialName}%'
        UNION ALL
        SELECT Activity_internal_id, [Refers To], [Responsible Institute], [Responsible Person], Technique, [Start Date], [End Date]
        FROM Exporting
        WHERE [Responsible Person] LIKE '%{partialName}%'
        UNION ALL
        SELECT Activity_internal_id, [Refers To], [Responsible Institute], [Responsible Person], Technique, [Start Date], [End Date]
        FROM Modelling
        WHERE [Responsible Person] LIKE '%{partialName}%'
        UNION ALL
        SELECT Activity_internal_id, [Refers To], [Responsible Institute], [Responsible Person], Technique, [Start Date], [End Date]
        FROM Optimising
        WHERE [Responsible Person] LIKE '%{partialName}%'
        UNION ALL
        SELECT Activity_internal_id, [Refers To], [Responsible Institute], [Responsible Person], Technique, [Start Date], [End Date]
        FROM Processing
        WHERE [Responsible Person] LIKE '%{partialName}%'
        """
        return self.executeQuery(sql_command)

    def getActivitiesUsingTool(self, partialName: str) -> DataFrame:
        sql_command = f"""
        SELECT A.Activity_internal_id, A.[Refers To], A.[Responsible Institute], A.[Responsible Person], A.Technique, A.[Start Date], A.[End Date]
        FROM Acquisition AS A
        INNER JOIN Tools AS T ON A.Tool_internal_id = T.Tool_internal_id
        WHERE T.Tool LIKE '%{partialName}%'
        UNION ALL
        SELECT A.Activity_internal_id, A.[Refers To], A.[Responsible Institute], A.[Responsible Person], A.Technique, A.[Start Date], A.[End Date]
        FROM Exporting AS A
        INNER JOIN Tools AS T ON A.Tool_internal_id = T.Tool_internal_id
        WHERE T.Tool LIKE '%{partialName}%'
        UNION ALL
        SELECT A.Activity_internal_id, A.[Refers To], A.[Responsible Institute], A.[Responsible Person], A.Technique, A.[Start Date], A.[End Date]
        FROM Modelling AS A
        INNER JOIN Tools AS T ON A.Tool_internal_id = T.Tool_internal_id
        WHERE T.Tool LIKE '%{partialName}%'
        UNION ALL
        SELECT A.Activity_internal_id, A.[Refers To], A.[Responsible Institute], A.[Responsible Person], A.Technique, A.[Start Date], A.[End Date]
        FROM Optimising AS A
        INNER JOIN Tools AS T ON A.Tool_internal_id = T.Tool_internal_id
        WHERE T.Tool LIKE '%{partialName}%'
        UNION ALL
        SELECT A.Activity_internal_id, A.[Refers To], A.[Responsible Institute], A.[Responsible Person], A.Technique, A.[Start Date], A.[End Date]
        FROM Processing AS A
        INNER JOIN Tools AS T ON A.Tool_internal_id = T.Tool_internal_id
        WHERE T.Tool LIKE '%{partialName}%'
        """
        return self.executeQuery(sql_command)

    def getActivitiesStartedAfter(self, date: str) -> DataFrame:
        sql_command = f"""
        SELECT Activity_internal_id, [Refers To], [Responsible Institute], [Responsible Person], Technique, [Start Date], [End Date]
        FROM Acquisition
        WHERE [Start Date] > '{date}'
        UNION ALL
        SELECT Activity_internal_id, [Refers To], [Responsible Institute], [Responsible Person], Technique, [Start Date], [End Date]
        FROM Exporting
        WHERE [Start Date] > '{date}'
        UNION ALL
        SELECT Activity_internal_id, [Refers To], [Responsible Institute], [Responsible Person], Technique, [Start Date], [End Date]
        FROM Modelling
        WHERE [Start Date] > '{date}'
        UNION ALL
        SELECT Activity_internal_id, [Refers To], [Responsible Institute], [Responsible Person], Technique, [Start Date], [End Date]
        FROM Optimising
        WHERE [Start Date] > '{date}'
        UNION ALL
        SELECT Activity_internal_id, [Refers To], [Responsible Institute], [Responsible Person], Technique, [Start Date], [End Date]
        FROM Processing
        WHERE [Start Date] > '{date}'
        """
        return self.executeQuery(sql_command)

    def getActivitiesEndedBefore(self, date: str) -> DataFrame:
        sql_command = f"""
        SELECT Activity_internal_id, [Refers To], [Responsible Institute], [Responsible Person], Technique, [Start Date], [End Date]
        FROM Acquisition
        WHERE [End Date] < '{date}'
        UNION ALL
        SELECT Activity_internal_id, [Refers To], [Responsible Institute], [Responsible Person], Technique, [Start Date], [End Date]
        FROM Exporting
        WHERE [End Date] < '{date}'
        UNION ALL
        SELECT Activity_internal_id, [Refers To], [Responsible Institute], [Responsible Person], Technique, [Start Date], [End Date]
        FROM Modelling
        WHERE [End Date] < '{date}'
        UNION ALL
        SELECT Activity_internal_id, [Refers To], [Responsible Institute], [Responsible Person], Technique, [Start Date], [End Date]
        FROM Optimising
        WHERE [End Date] < '{date}'
        UNION ALL
        SELECT Activity_internal_id, [Refers To], [Responsible Institute], [Responsible Person], Technique, [Start Date], [End Date]
        FROM Processing
        WHERE [End Date] < '{date}'
        """
        return self.executeQuery(sql_command)

    def getAcquisitionsByTechnique(self, partialName: str) -> DataFrame:
        sql_command = """
        SELECT Activity_internal_id, [Refers To], [Responsible Institute], [Responsible Person], Technique, [Start Date], [End Date]
        FROM Acquisition
        WHERE LOWER(Technique) LIKE '%{}%'
        ORDER BY [Refers To]
        """.format(partialName.lower())
        return self.executeQuery(sql_command)

# Instantiate ProcessDataQueryHandler
query_handler = ProcessDataQueryHandler(dbPathOrUrl="relational_database.db")



# Class to interact with Blazegraph
# please remove semicolons at the end of the lines

class MetadataQueryHandler(QueryHandler):
    def __init__(self, getById):
        self.getById = getById
    def getAllPeople(self, Allpeople):
        Allpeople_csv = self.getById("Author")
        Allpeople = pd.DataFrame(Allpeople_csv)
        return Allpeople

    def getAllCulturalHeritageObjects(self, HeritageObjects):
        HeritageObjects = pd.read_csv("meta.csv")
        return HeritageObjects

    def getAuthorsOfCulturalHeritageObject(self, objectid, AuthorsOfCulturalHeritageObject):
        AuthorsOfCulturalHeritageObject = pd.read_csv("meta.csv")
        Authorid = AuthorsOfCulturalHeritageObject[AuthorsOfCulturalHeritageObject["objectid"] == objectid]
        return Authorid

    def getCulturalHeritageObjectsAuthoredBy(self, personid, CulturalHeritageObjectsAuthoredBy):
        CulturalHeritageObjectsAuthoredBy = pd.read_csv("meta.csv")
        AuthoredBy = CulturalHeritageObjectsAuthoredBy[getAuthorsOfCulturalHeritageObjects(input_id)] == [input_id, CulturalHeritageObjectsAuthoredBy]
        return AuthoredBy

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
