from typing import Optional
import os

import json
import csv


# Imports for relational db
from sqlite3 import connect
from json import load

import sqlite3

# Imports for graph db
from rdflib import RDF, Graph, URIRef, Literal
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
import SPARQLWrapper

# Imports for both
import pandas as pd
from pandas import DataFrame, concat, read_csv, read_sql, Series

# Date-time manipulation
from datetime import datetime

# CH Objects Metadata classes

class IdentifiableEntity(object):
    def __init__(self, id: str):
        self.id = id

    def getId(self):
        return self.id

class Person(IdentifiableEntity):
    def __init__(self, id: str, name: str):
        self.name = name
        super().__init__(id)

    def getName(self):
        return self.name

class CulturalHeritageObject(IdentifiableEntity):
    def __init__(self, id: str, title: str, date: str, hasAuthor: list[Person], owner: str, place: str):
        self.title = title
        self.date = date
        self.owner = owner
        self.place = place
        self.hasAuthor = hasAuthor or []

        super().__init__(id)

    def getTitle(self) -> str:
        return self.title

    def getDate(self) -> Optional[str]:
        return self.date

    def getAuthors(self) -> list[Person]: #Returns a list of authors (Person objects)
        return self.hasAuthor
    
    def getOwner(self) -> str:
        return self.owner
    
    def getPlace(self) -> str:
        return self.place

class NauticalChart(CulturalHeritageObject):
    pass

class ManuscriptPlate(CulturalHeritageObject):
    pass

class ManuscriptVolume(CulturalHeritageObject):
    pass

class PrintedVolume(CulturalHeritageObject):
    pass

class PrintedMaterial(CulturalHeritageObject):
    pass

class Herbarium(CulturalHeritageObject):
    pass

class Specimen(CulturalHeritageObject):
    pass

class Painting(CulturalHeritageObject):
    pass

class Model(CulturalHeritageObject):
    pass

class Map(CulturalHeritageObject):
    pass

# Processes Data classes
class Activity(object): # Lucrezia
    def __init__(self, refersTo_cho: CulturalHeritageObject, institute: str, person: str, start: str, end: str, tool: set = set()):
        self.refersTo_cho = refersTo_cho
        self.institute = institute
        self.person = person
        self.tool = tool
        self.start = start
        self.end = end

    def getResponsibleInstitute(self) -> str:
        return self.institute
    
    def getResponsiblePerson(self) -> Optional[str]:
        return self.person
    
    def getTools(self) -> set: # getTool has arity zero or more [0..*]
        return self.tool
    
    def getStartDate(self) -> Optional[str]:
        return self.start

    def getEndDate(self) -> Optional[str]:
        return self.end
    
    def getRefersTo_cho(self) -> CulturalHeritageObject:
        return self.refersTo_cho

class Acquisition(Activity):
    def __init__(self, refersTo_cho: CulturalHeritageObject, institute: str, person: str, start: str, end: str, technique: str, tool: set = set()):
        super().__init__(refersTo_cho, institute, person, start, end, tool)
        self.technique = technique
    
    def getTechnique(self) -> str:
        return self.technique

class Processing(Activity):
    pass

class Modelling(Activity):
    pass

class Optimising(Activity):
    pass

class Exporting(Activity):
    pass

# ======================================================== #

# Basic Handlers classes
# Hubert
class Handler(object):
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
        pass # never accessed here and overriden in child classes

class QueryHandler(Handler): 
    def __init__(self, dbPathOrUrl=""):  # Provide a default value for dbPathOrUrl
        super().__init__()
        self.dbPathOrUrl = dbPathOrUrl

    def getById(self, id: str):
        pass # never accessed here and overriden in child classes
    
# Class to upload data from JSON to SQLite database 
class ProcessDataUploadHandler(UploadHandler):    # Lucrezia
    def __init__(self, db_name=""):
        super().__init__()
        self.db_name = db_name
        self.activity_dfs = {}
        self.tools_df = pd.DataFrame()
 
    def process_data(self, json_data):
        file_path = os.path.join("data", "process.json")
        with open(file_path, "r", encoding="utf-8") as file:
            json_data = json.load(file)
 
        object_id_mapping = self.map_object_ids(json_data)
        self.activity_dfs, self.tools_df = self.create_dataframes(json_data, object_id_mapping)
 
    def map_object_ids(self, json_data):
        object_id_mapping = {}
        for item in json_data:
            object_id = item['object id']
            object_internal_id = f"CH Object-{object_id}"
            object_id_mapping[object_id] = object_internal_id
        return object_id_mapping
 
    def create_dataframes(self, json_data, object_id_mapping):
        activity_dfs = {} # a dictionary to store DataFrames for each activity type
        tools_data = [] # a list to store tool-related data
        for activity_type in json_data[0].keys():
            if activity_type != 'object id':
                activity_data = []
                activity_count = 1
                for item in json_data:
                    activity = item.get(activity_type, {})
                    if activity:
                        object_id = item['object id']
                        activity_internal_id = f"{activity_type.capitalize()}-{activity_count:02d}"
                        activity_count += 1
                        activity_data.append({
                            "Activity_internal_id": activity_internal_id,
                            "Refers To": object_id_mapping.get(object_id, ""),
                            "Responsible Institute": activity.get("responsible institute", ""),
                            "Responsible Person": activity.get("responsible person", ""),
                            "Technique": activity.get("technique", "") if activity_type == "acquisition" else "",
                            "Tool_internal_id": f"{activity_internal_id}-tool",
                            "Start Date": activity.get("start date", ""),
                            "End Date": activity.get("end date", ""),
                        })
                        if 'tool' in activity:
                            for tool in activity['tool']:
                                tools_data.append({
                                    "Tool_internal_id": f"{activity_internal_id}-tool",
                                    "Tool": tool,
                                    "Activity_internal_id": activity_internal_id
                                })
                activity_dfs[activity_type] = pd.DataFrame(activity_data)
        tools_df = pd.DataFrame(tools_data)
        return activity_dfs, tools_df
 
    def pushDataToDb(self, path):
        with open(path, "r", encoding="utf-8") as file:
            json_data = json.load(file)
 
        object_id_mapping = self.map_object_ids(json_data)
        self.activity_dfs, self.tools_df = self.create_dataframes(json_data, object_id_mapping)
        try:
            with connect(self.dbPathOrUrl) as conn:
                for activity_type, df in self.activity_dfs.items():
                    df.to_sql(activity_type.capitalize(), conn, if_exists='replace', index=False)
                self.tools_df.to_sql('Tools', conn, if_exists='replace', index=False)
            return True  # Return True if all operations succeed
        except Exception as e:
            print(f"Error occurred while pushing data to DB: {str(e)}")
            return False  # Return False if any error occurs
 
    def handle_duplicates(self, df, conn, table_name):
        existing_data = pd.read_sql(f"SELECT * FROM {table_name}", conn)
        df = df[~df.duplicated(keep='first')]
        if not existing_data.empty:
            df = pd.concat([existing_data, df]).drop_duplicates(keep='first')
        return df

# Class to upload CSV files to Blazegraph
class MetadataUploadHandler(UploadHandler):     # Hubert
    def __init__(self):
        self.dbPathOrUrl = ""

    def pushDataToDb(self, path):
        try:
            # defining our base url
            base_url = "https://comp-data.github.io/res/";
            graph = Graph();
            # reading the data from the csv file and storing them into a dataframe
            meta = pd.read_csv(path, 
                            keep_default_na=False, 
                            dtype={
                                "Id": "string",
                                "Type": "string",
                                "Title": "string",
                                "Date": "string",
                                "Author": "string",
                                "Owner": "string",
                                "Place": "string"
                            });

            # classes of resources
            NauticalChart = URIRef("http://dbpedia.org/resource/Nautical_chart");
            ManuscriptPlate = URIRef(base_url + "Manuscript_plate");
            ManuscriptVolume = URIRef(base_url + "Manuscript_volume");
            PrintedVolume = URIRef(base_url + "Printed_volume");
            PrintedMaterial = URIRef("http://dbpedia.org/resource/Printed_material");
            Herbarium = URIRef("http://dbpedia.org/resource/Herbarium");
            Specimen = URIRef("http://dbpedia.org/resource/Specimen");
            Painting = URIRef("http://dbpedia.org/resource/Painting");
            Model = URIRef("http://dbpedia.org/resource/Model");
            Map = URIRef("http://dbpedia.org/resource/Map");
            Person = URIRef("http://dbpedia.org/resource/Person");

            # attributes related to classes
            id = URIRef("https://schema.org/identifier");
            type = URIRef("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
            title = URIRef("https://schema.org/name");
            date = URIRef("https://schema.org/dateCreated");
            author = URIRef("https://schema.org/author");
            owner = URIRef("https://schema.org/copyrightHolder");
            place = URIRef("https://schema.org/spatial");

            # relations among classes

            for idx, row in meta.iterrows():
                local_id = "itemid-" + str(idx);
                subj = URIRef(base_url + local_id);
                # assigning object id
                graph.add((subj, id, Literal(str(row["Id"]))));
                # assigning resource classes to the object
                if row["Type"] == "Nautical chart":
                    graph.add((subj, RDF.type, NauticalChart));
                elif row["Type"] == "Manuscript plate":
                    graph.add((subj, RDF.type, ManuscriptPlate));
                elif row["Type"] == "Manuscript volume":
                    graph.add((subj, RDF.type, ManuscriptVolume));
                elif row["Type"] == "Printed volume":
                    graph.add((subj, RDF.type, PrintedVolume));
                elif row["Type"] == "Printed material":
                    graph.add((subj, RDF.type, PrintedMaterial));
                elif row["Type"] == "Herbarium":
                    graph.add((subj, RDF.type, Herbarium));
                elif row["Type"] == "Specimen":
                    graph.add((subj, RDF.type, Specimen));
                elif row["Type"] == "Painting":
                    graph.add((subj, RDF.type, Painting));
                elif row["Type"] == "Model":
                    graph.add((subj, RDF.type, Model));
                elif row["Type"] == "Map":
                    graph.add((subj, RDF.type, Map));
                # assigning title
                graph.add((subj, title, Literal(row["Title"].strip())));
                # assigning date
                if row["Date"] != "":
                    graph.add((subj, date, Literal(str(row["Date"]))));
                # assigning author
                if row["Author"] != "":
                    # strip author string to name and author id
                    full_author = row["Author"].strip('\"')
                    author_id = full_author[full_author.find(":")+len(":"):full_author.rfind(")")]
                    author_name = full_author.split("(")[0].strip()
                    author_res_id = base_url + "Person/" + author_id
                    subj_author = URIRef(author_res_id)
                    graph.add((subj, author, subj_author))
                    graph.add((subj_author, title, Literal(str(author_name))))
                    graph.add((subj_author, id, Literal(str(author_id))))
                # assigning owner
                if row["Owner"] != "":
                    graph.add((subj, owner, Literal(row["Owner"])))
                # assigning place
                if row["Place"] != "":
                    graph.add((subj, place, Literal(row["Place"])))

            store = SPARQLUpdateStore()

            # It opens the connection with the SPARQL endpoint instance
            store.open((self.dbPathOrUrl, self.dbPathOrUrl ))

            for triple in graph.triples((None, None, None)):
                store.add(triple)
                
            # Once finished, remeber to close the connection
            store.close()
            return True
    
        except Exception as e:
            print("The upload of data to Blazegraph failed: " + str(e))
            return False

# ======================================================== #

# Class to interact with SQLite database 
class ProcessDataQueryHandler(QueryHandler):        # Lucrezia
    def __init__(self, dbPathOrUrl=""):
        super().__init__()
        self.dbPathOrUrl = dbPathOrUrl
        self.db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), self.dbPathOrUrl))

    def getById(self, id:str):
        return pd.DataFrame() # return empty dataframe as no IdentifiableEntity in relational db
    
    def executeQuery(self, sql_command):
        connection = connect(self.dbPathOrUrl);
        cursor = connection.cursor();
        cursor.execute(sql_command);
        df = pd.DataFrame(cursor.fetchall(), columns = [description[0] for description in cursor.description]);
        #df.columns = [description[0] for description in cursor.description]; # setting column names with list comprehension because sqlite lacks a normal reference to column names
        connection.close();
        return df;
    
        with connect(self.dbPathOrUrl) as conn:
            cursor = conn.cursor()
            cursor.execute(sql_command)
            columns = [description[0] for description in cursor.description]
            dataframe_query = pd.DataFrame(cursor.fetchall(), columns=columns)
        return dataframe_query

    
    def getAllActivities(self):
        sql_command = """
        SELECT Activity_internal_id, [Refers To], [Responsible Institute], [Responsible Person], Technique, [Start Date], [End Date] FROM Acquisition;
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
    
# Class to interact with Blazegraph database
class MetadataQueryHandler(QueryHandler):
    def __init__(self):
        self.dbPathOrUrl = "";
    
    def execute_sparql_query(self, query):      # helper method
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

    def getById(self, id):
        person_query = "SELECT DISTINCT ?uri ?name ?id WHERE { ?object <https://schema.org/author> ?uri.  ?uri <https://schema.org/name> ?name.  ?uri <https://schema.org/identifier> ?id. ?uri <https://schema.org/identifier> '%s'. }" % id;
        object_query = "SELECT DISTINCT ?object ?id ?type ?title ?date ?owner ?place ?author ?author_name ?author_id WHERE { ?object <https://schema.org/identifier> ?id.  ?object <https://schema.org/identifier> '%s'. ?object <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?type. ?object <https://schema.org/name> ?title. ?object <https://schema.org/dateCreated> ?date. ?object <https://schema.org/copyrightHolder> ?owner. ?object <https://schema.org/spatial> ?place. ?object <https://schema.org/author> ?author. ?author <https://schema.org/name> ?author_name. ?author <https://schema.org/identifier> ?author_id.}" % id;
        person_df = self.execute_sparql_query(person_query);
        object_df = self.execute_sparql_query(object_query);
        if len(object_df.index) > 0: # if objects exist return objects
            return object_df;
        return person_df; # otherwise persons exists or we return an empty dataframe

    def getAllPeople(self):
        query = """
            SELECT DISTINCT ?uri ?author_name ?author_id
            WHERE {
                ?object <https://schema.org/author> ?author .
                ?author <https://schema.org/identifier> ?author_id .
                ?author <https://schema.org/name> ?author_name .
            }
            """
        return self.execute_sparql_query(query)

    def getAllCulturalHeritageObjects(self):
        query = """
            SELECT DISTINCT ?object ?id ?type ?title ?date ?owner ?place ?author ?author_name ?author_id 
            WHERE { 
                ?object <https://schema.org/identifier> ?id. 
                ?object <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?type. 
                ?object <https://schema.org/name> ?title. 
                ?object <https://schema.org/copyrightHolder> ?owner. 
                ?object <https://schema.org/spatial> ?place. 
            OPTIONAL{ ?object <https://schema.org/dateCreated> ?date. } 
            OPTIONAL{ 
                ?object <https://schema.org/author> ?author. 
                ?author <https://schema.org/name> ?author_name. 
                ?author <https://schema.org/identifier> ?author_id.
            }}
            """
        return self.execute_sparql_query(query)

    def getAuthorsOfCulturalHeritageObject(self, object_id: str) -> pd.DataFrame: #Giorgia
        query = """
            SELECT DISTINCT ?author ?author_name ?author_id 
            WHERE { 
                ?object <https://schema.org/identifier> '%s'. 
                ?object <https://schema.org/author> ?author. 
                ?author <https://schema.org/name> ?author_name. 
                ?author <https://schema.org/identifier> ?author_id. 
                }""" % object_id
        return self.execute_sparql_query(query)

    def getCulturalHeritageObjectsAuthoredBy(self, personId): #Giorgia
        query = """
            SELECT DISTINCT ?object ?id ?type ?title ?date ?owner ?place ?author ?author_name ?author_id 
            WHERE { 
                ?object <https://schema.org/identifier> ?id. 
                ?object <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?type. 
                ?object <https://schema.org/name> ?title. 
                ?object <https://schema.org/copyrightHolder> ?owner. 
                ?object <https://schema.org/spatial> ?place. 
                ?object <https://schema.org/author> ?author. 
                ?author <https://schema.org/name> ?author_name.
                ?author <https://schema.org/identifier> ?author_id.
                ?author <https://schema.org/identifier> '%s'.
            OPTIONAL{ ?object <https://schema.org/dateCreated> ?date. }}
            """ % personId
        return self.execute_sparql_query(query)

# ======================================================== #

# Mashup classes

class BasicMashup(object):  #Hubert
    def __init__(self):
        self.metadataQuery = [];
        self.processQuery = [];

    # helper method to reduce code clutter # Hubert
    def createActivityList(self, df): # Hubert 
        activities = []
        
        # formatted version Lucrezia
        for idx, row in df.iterrows():
            cho = self.getEntityById(row["Refers To"].split('-')[1])
            if "acquisition" in row["Activity_internal_id"]:
                activity = Acquisition(
                    cho,
                    row["Responsible Institute"],
                    row["Responsible Person"],
                    row["Tool"],
                    row["Start Date"],
                    row["End Date"],
                    row["Technique"]
                )
                activities.append(activity)
            elif "processing" in row["Activity_internal_id"]:
                activity = Processing(
                    cho,
                    row["Responsible Institute"],
                    row["Responsible Person"],
                    row["Tool"],
                    row["Start Date"],
                    row["End Date"]
                )
                activities.append(activity)
            elif "modelling" in row["Activity_internal_id"]:
                activity = Modelling(
                    cho,
                    row["Responsible Institute"],
                    row["Responsible Person"],
                    row["Tool"],
                    row["Start Date"],
                    row["End Date"]
                )
                activities.append(activity)
            elif "optimising" in row["Activity_internal_id"]:
                activity = Optimising(
                    cho,
                    row["Responsible Institute"],
                    row["Responsible Person"],
                    row["Tool"],
                    row["Start Date"],
                    row["End Date"]
                )
                activities.append(activity)
            elif "exporting" in row["Activity_internal_id"]:
                activity = Exporting(
                    cho,
                    row["Responsible Institute"],
                    row["Responsible Person"],
                    row["Tool"],
                    row["Start Date"],
                    row["End Date"]
                )
                activities.append(activity)
        
        return activities

    # helper method to reduce code clutter
    def createObjectList(self, cho_df): # Hubert
        cho_list = [];
        for idx, row in cho_df.iterrows():
            if "Nautical_chart" in row["type"]:
                obj = NauticalChart(row["id"], row["title"], row["date"], [Person(row["author_id"], row["author_name"])], row["owner"], row["place"]);
                cho_list.append(obj)
            elif "Manuscript_plate" in row["type"]:
                obj = ManuscriptPlate(row["id"], row["title"], row["date"], [Person(row["author_id"], row["author_name"])], row["owner"], row["place"]);
                cho_list.append(obj)
            elif "Manuscript_volume" in row["type"]:
                obj = ManuscriptVolume(row["id"], row["title"], row["date"], [Person(row["author_id"], row["author_name"])], row["owner"], row["place"]);
                cho_list.append(obj)
            elif "Printed_volume" in row["type"]:
                obj = PrintedVolume(row["id"], row["title"], row["date"], [Person(row["author_id"], row["author_name"])], row["owner"], row["place"]);
                cho_list.append(obj)
            elif "Printed_material" in row["type"]:
                obj = PrintedMaterial(row["id"], row["title"], row["date"], [Person(row["author_id"], row["author_name"])], row["owner"], row["place"]);
                cho_list.append(obj)
            elif "Herbarium" in row["type"]:
                obj = Herbarium(row["id"], row["title"], row["date"], [Person(row["author_id"], row["author_name"])], row["owner"], row["place"]);
                cho_list.append(obj)
            elif "Specimen" in row["type"]:
                obj = Specimen(row["id"], row["title"], row["date"], [Person(row["author_id"], row["author_name"])], row["owner"], row["place"]);
                cho_list.append(obj)
            elif "Painting" in row["type"]:
                obj = Painting(row["id"], row["title"], row["date"], [Person(row["author_id"], row["author_name"])], row["owner"], row["place"]);
                cho_list.append(obj)
            elif "Model" in row["type"]:
                obj = Model(row["id"], row["title"], row["date"], [Person(row["author_id"], row["author_name"])], row["owner"], row["place"]);
                cho_list.append(obj)
            elif "Map" in row["type"]:
                obj = Map(row["id"], row["title"], row["date"], [Person(row["author_id"], row["author_name"])], row["owner"], row["place"]);
                cho_list.append(obj)
        return cho_list;

    def cleanMetadataHandlers(self): # Hubert
        self.metadataQuery = [];
        return True;

    def cleanProcessHandlers(self): # Hubert
        self.processQuery = [];
        return True;

    def addMetadataHandler(self, handler: MetadataQueryHandler):
        self.metadataQuery.append(handler);
        return True;

    def addProcessHandler(self, handler: ProcessDataQueryHandler):
        self.processQuery.append(handler);
        return True;

    def getEntityById(self, entity_id: str) -> IdentifiableEntity: #Giorgia
        if not self.metadataQuery:
            return None
        
        handler = self.metadataQuery[0]
        df = handler.getById(entity_id)
        
        if df.empty: #check if it's empty
            return None
        
        if 'type' in df.columns: #check for cho
            cho_list = self.createObjectList(df)
            if cho_list:
                return cho_list[0]  
        
        if 'name' in df.columns and 'author_id' in df.columns: 
            return Person(df.iloc[0]["author_id"], df.iloc[0]["author_name"])
        
        return None

    def getAllPeople(self):
        person_list = [];
        if len(self.metadataQuery) > 0:
            person_df = self.metadataQuery[0].getAllPeople();
            for idx, row in person_df.iterrows():
                person = Person(row["author_id"], row["author_name"]);
                person_list.append(person);
        return person_list;
    
    def getAllCulturalHeritageObjects(self) -> list[CulturalHeritageObject]:  # Iheb
       cho_list = []
       if len(self.metadataQuery) > 0:
        cho_df = self.metadataQuery[0].getAllCulturalHeritageObjects()
        cho_list =  self.createObjectList(cho_df)
       return cho_list

    def getAuthorsOfCulturalHeritageObject(self, objectId: str) -> list[Person]:
        author_list = []
        if len(self.metadataQuery) > 0:
            author_df = self.metadataQuery[0].getAuthorsOfCulturalHeritageObject(objectId)
            for idx, row in author_df.iterrows():
                person = Person(row["author_id"], row["author_name"])
                author_list.append(person)
        return author_list

    def getCulturalHeritageObjectsAuthoredBy(self, AuthorId: str) -> list[CulturalHeritageObject]:  # Iheb
       cho_list = []
       if len(self.metadataQuery) > 0:
        cho_df = self.metadataQuery[0].getCulturalHeritageObjectsAuthoredBy(AuthorId)
        cho_list =  self.createObjectList(cho_df)
        return cho_list

    def getAllActivities(self): # Giorgia
        activities = []
        if len(self.processQuery) > 0:
            activities_df = self.processQuery[0].getAllActivities();
            activities = self.createActivityList(activities_df);
        return activities;
    
    def getActivitiesByResponsibleInstitution(self, partialName: str) -> list[Activity]:
        activities = []  
        if len(self.processQuery) > 0:  # check handlers 
            activities_df = self.processQuery[0].getActivitiesByResponsibleInstitution(partialName)  
            activities = self.createActivityList(activities_df) 
        return activities  

    def getActivitiesByResponsiblePerson(self, partialName: str) -> list[Activity]:  # Giorgia
        activities = []  
        if len(self.processQuery) > 0:  #check for handlers 
            activities_df = self.processQuery[0].getActivitiesByResponsiblePerson(partialName)  
            activities = self.createActivityList(activities_df) 
        return activities  

    def getActivitiesUsingTool(self, partial_name: str):  # Giorgia
        activities = []
        if len(self.processQuery) > 0:
            activities_df = self.processQuery[0].getActivitiesUsingTool(partial_name);
            activities = self.createActivityList(activities_df);
        return activities;

    def getActivitiesStartedAfter(self, date: str) -> list[Activity]:
        activities = []  
        if len(self.processQuery) > 0:  
            activities_df = self.processQuery[0].getActivitiesStartedAfter(date)  
            activities = self.createActivityList(activities_df) 
        return activities  
        
    def getActivitiesEndedBefore(self, date: str) -> list[Activity]:
        activities = []  
        if len(self.processQuery) > 0:  
            activities_df = self.processQuery[0].getActivitiesEndedBefore(date)  
            activities = self.createActivityList(activities_df) 
        return activities

    def getAcquisitionsByTechnique(self, technique: str) -> list[Acquisition]: #Giorgia
        activities = []  
        if len(self.processQuery) > 0:  
            activities_df = self.processQuery[0].getAcquisitionsByTechnique(technique)  
            activities = self.createActivityList(activities_df) 
        return activities 

class AdvancedMashup(BasicMashup):
    def getActivitiesOnObjectsAuthoredBy(self, personId: str) -> list[Activity]: #Giorgia
        activities = []  
        
        cho_list = self.getAllCulturalHeritageObjects()
        filt_cho = []
        for item in cho_list:
            person = item.getAuthors()
            person_id = person[0].id
            if person_id == personId and item not in filt_cho:
                filt_cho.append(item)
       
        activities_list = self.getAllActivities()
        for item in activities_list: 
            if item.refersTo_cho in filt_cho:
                activities.append(item)
        return activities 
    
    def getObjectsHandledByResponsiblePerson(self, partialName: str) -> list[CulturalHeritageObject]:  #giorgia
        objects = []
        if len(self.processQuery) > 0:
            institutions_df = self.processQuery[0].getActivitiesByResponsiblePerson(partialName)
            activities = self.createActivityList(institutions_df)
            if len(self.metadataQuery) > 0:
                objects_df = self.metadataQuery[0].getAllCulturalHeritageObjects()
                object_list = self.createObjectList(objects_df)
                object_ids = []
                for activity in activities:
                    activity_id = activity.refersTo_cho.id
                    if activity_id not in object_ids:
                        object_ids.append(activity_id)
                for cho in object_list:
                    if cho.id in object_ids and cho not in objects:
                        objects.append(cho)
        return objects

    def getObjectsHandledByResponsibleInstitution(self, partialName: str) -> list[CulturalHeritageObject]:  # Iheb
        objects = []
        if len(self.processQuery) > 0:
            institutions_df = self.processQuery[0].getActivitiesByResponsibleInstitution(partialName)
            activities = self.createActivityList(institutions_df)
            if len(self.metadataQuery) > 0:
                objects_df = self.metadataQuery[0].getAllCulturalHeritageObjects()
                object_list = self.createObjectList(objects_df)
                object_ids = []
                for activity in activities:
                    activity_id = activity.refersTo_cho.id
                    if activity_id not in object_ids:
                        object_ids.append(activity_id)
                for cho in object_list:
                    if cho.id in object_ids and cho not in objects:
                        objects.append(cho)
        return objects

    def getAuthorsOfObjectsAcquiredInTimeFrame(self, start: str, end: str) -> list[Person]:  # Iheb
        authors = []
        if len(self.processQuery) > 0:
            activities_before = self.getActivitiesEndedBefore(end)
            activities_after = self.getActivitiesStartedAfter(start)
            activity_list = [value for value in activities_before if value in activities_after]
            
            for activity in activity_list:
                if activity.refersTo_cho.person[0] and activity.refersTo_cho.person[0] not in authors: 
                    author = activity.refersTo_cho.person[0]    
                    authors.append(author)
        return authors

