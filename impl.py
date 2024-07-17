from typing import Optional
import os
import json
import csv


# Imports for relational db
from sqlite3 import connect
from json import load

# Imports for graph db
from rdflib import RDF, Graph, URIRef, Literal
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
import SPARQLWrapper

# Imports for both
import pandas as pd
from pandas import DataFrame, concat, read_csv, read_sql, Series
import numpy as np

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
    
    def refersTo(self) -> CulturalHeritageObject:
        return self.refersTo_cho

class Acquisition(Activity):
    def __init__(self, refersTo_cho: CulturalHeritageObject, institute: str, person: str, start: str, end: str, technique: str, tool):
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
    def __init__(self, db_name="relational.db"):
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
                        start_date = activity.get("start date", "")
                        end_date = activity.get("end date", "")
                        responsible_institute = activity.get("responsible institute", "")
                        responsible_person = activity.get("responsible person", "")
                        if start_date != "" or end_date != "" or responsible_institute != "" or responsible_person != "":
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
                                tools_string = '🤪 '.join(str(tool) for tool in activity['tool'])
                                tools_data.append({
                                        "Tool_internal_id": f"{activity_internal_id}-tool",
                                        "Tool": tools_string,
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
                    df = self.handle_duplicates(df, conn, activity_type.capitalize())
                    df.to_sql(activity_type.capitalize(), conn, if_exists='replace', index=False, dtype="string")
                self.tools_df = self.handle_duplicates(self.tools_df, conn, 'Tools')
                self.tools_df.to_sql('Tools', conn, if_exists='replace', index=False, dtype="string")
            return True  # Return True if all operations succeed
        except Exception as e:
            print(f"Error occurred while pushing data to DB: {str(e)}")
            return False  # Return False if any error occurs
 
    def handle_duplicates(self, df, conn, table_name):
        try:
            existing_data = pd.read_sql(f"SELECT * FROM {table_name}", conn)
        except Exception as e:
            existing_data = pd.DataFrame();
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
            #removing duplicates based on Id column, keeping the first instance
            meta.drop_duplicates(subset=["Id"], keep="first", inplace=True, ignore_index=True)
            
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
                    full_author = row["Author"].strip('\"').split(";")
                    for author_string in full_author:
                        author_id = author_string.split("(")[1]
                        author_id = author_id[:-1].strip()
                        author_name = author_string.split("(")[0].strip()
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

    
    def getAllActivities(self):
        sql_command = """
        SELECT A.Activity_internal_id, A.[Refers To], A.[Responsible Institute], A.[Responsible Person], A.Technique, A.[Start Date], A.[End Date], T.Tool
        FROM Acquisition AS A
	    INNER JOIN Tools AS T ON A.Tool_internal_id = T.Tool_internal_id
        UNION
        SELECT A.Activity_internal_id, A.[Refers To], A.[Responsible Institute], A.[Responsible Person], A.Technique, A.[Start Date], A.[End Date], T.Tool
        FROM Exporting AS A
	    INNER JOIN Tools AS T ON A.Tool_internal_id = T.Tool_internal_id
        UNION
        SELECT A.Activity_internal_id, A.[Refers To], A.[Responsible Institute], A.[Responsible Person], A.Technique, A.[Start Date], A.[End Date], T.Tool
        FROM Modelling AS A
	    INNER JOIN Tools AS T ON A.Tool_internal_id = T.Tool_internal_id
        UNION
        SELECT A.Activity_internal_id, A.[Refers To], A.[Responsible Institute], A.[Responsible Person], A.Technique, A.[Start Date], A.[End Date], T.Tool
        FROM Optimising AS A
	    INNER JOIN Tools AS T ON A.Tool_internal_id = T.Tool_internal_id
        UNION
        SELECT A.Activity_internal_id, A.[Refers To], A.[Responsible Institute], A.[Responsible Person], A.Technique, A.[Start Date], A.[End Date], T.Tool
        FROM Processing AS A
	    INNER JOIN Tools AS T ON A.Tool_internal_id = T.Tool_internal_id;
        """
        return self.executeQuery(sql_command)
    
    def getActivitiesByResponsibleInstitution(self, partialName):
        sql_command = f"""
        SELECT A.Activity_internal_id, A.[Refers To], A.[Responsible Institute], A.[Responsible Person], A.Technique, A.[Start Date], A.[End Date], T.Tool
        FROM Acquisition AS A
	    INNER JOIN Tools AS T ON A.Tool_internal_id = T.Tool_internal_id
	    WHERE A.[Responsible Institute] LIKE '%{partialName}%'
        UNION
        SELECT A.Activity_internal_id, A.[Refers To], A.[Responsible Institute], A.[Responsible Person], A.Technique, A.[Start Date], A.[End Date], T.Tool
        FROM Exporting AS A
	    INNER JOIN Tools AS T ON A.Tool_internal_id = T.Tool_internal_id
	    WHERE A.[Responsible Institute] LIKE '%{partialName}%'
        UNION
        SELECT A.Activity_internal_id, A.[Refers To], A.[Responsible Institute], A.[Responsible Person], A.Technique, A.[Start Date], A.[End Date], T.Tool
        FROM Modelling AS A
	    INNER JOIN Tools AS T ON A.Tool_internal_id = T.Tool_internal_id
	    WHERE A.[Responsible Institute] LIKE '%{partialName}%'
        UNION
        SELECT A.Activity_internal_id, A.[Refers To], A.[Responsible Institute], A.[Responsible Person], A.Technique, A.[Start Date], A.[End Date], T.Tool
        FROM Optimising AS A
	    INNER JOIN Tools AS T ON A.Tool_internal_id = T.Tool_internal_id
	    WHERE A.[Responsible Institute] LIKE '%{partialName}%'
        UNION
        SELECT A.Activity_internal_id, A.[Refers To], A.[Responsible Institute], A.[Responsible Person], A.Technique, A.[Start Date], A.[End Date], T.Tool
        FROM Processing AS A
	    INNER JOIN Tools AS T ON A.Tool_internal_id = T.Tool_internal_id
	    WHERE A.[Responsible Institute] LIKE '%{partialName}%';
        """
        return self.executeQuery(sql_command)
    
    def getActivitiesByResponsiblePerson(self, partialName: str) -> DataFrame:
        sql_command = f"""
        SELECT A.Activity_internal_id, A.[Refers To], A.[Responsible Institute], A.[Responsible Person], A.Technique, A.[Start Date], A.[End Date], T.Tool
        FROM Acquisition AS A
	    INNER JOIN Tools AS T ON A.Tool_internal_id = T.Tool_internal_id
	    WHERE A.[Responsible Person] LIKE '%{partialName}%'
        UNION
        SELECT A.Activity_internal_id, A.[Refers To], A.[Responsible Institute], A.[Responsible Person], A.Technique, A.[Start Date], A.[End Date], T.Tool
        FROM Exporting AS A
	    INNER JOIN Tools AS T ON A.Tool_internal_id = T.Tool_internal_id
	    WHERE A.[Responsible Person] LIKE '%{partialName}%'
        UNION
        SELECT A.Activity_internal_id, A.[Refers To], A.[Responsible Institute], A.[Responsible Person], A.Technique, A.[Start Date], A.[End Date], T.Tool
        FROM Modelling AS A
	    INNER JOIN Tools AS T ON A.Tool_internal_id = T.Tool_internal_id
	    WHERE A.[Responsible Person] LIKE '%{partialName}%'
        UNION
        SELECT A.Activity_internal_id, A.[Refers To], A.[Responsible Institute], A.[Responsible Person], A.Technique, A.[Start Date], A.[End Date], T.Tool
        FROM Optimising AS A
	    INNER JOIN Tools AS T ON A.Tool_internal_id = T.Tool_internal_id
	    WHERE A.[Responsible Person] LIKE '%{partialName}%'
        UNION
        SELECT A.Activity_internal_id, A.[Refers To], A.[Responsible Institute], A.[Responsible Person], A.Technique, A.[Start Date], A.[End Date], T.Tool
        FROM Processing AS A
	    INNER JOIN Tools AS T ON A.Tool_internal_id = T.Tool_internal_id
	    WHERE A.[Responsible Person] LIKE '%{partialName}%';
        """
        return self.executeQuery(sql_command)

    def getActivitiesUsingTool(self, partialName: str) -> DataFrame:
        sql_command = f"""
        SELECT A.Activity_internal_id, A.[Refers To], A.[Responsible Institute], A.[Responsible Person], A.Technique, A.[Start Date], A.[End Date], T.Tool
        FROM Acquisition AS A
	    INNER JOIN Tools AS T ON A.Tool_internal_id = T.Tool_internal_id
	    WHERE T.Tool LIKE '%{partialName}%'
        UNION
        SELECT A.Activity_internal_id, A.[Refers To], A.[Responsible Institute], A.[Responsible Person], A.Technique, A.[Start Date], A.[End Date], T.Tool
        FROM Exporting AS A
	    INNER JOIN Tools AS T ON A.Tool_internal_id = T.Tool_internal_id
	    WHERE T.Tool LIKE '%{partialName}%'
        UNION
        SELECT A.Activity_internal_id, A.[Refers To], A.[Responsible Institute], A.[Responsible Person], A.Technique, A.[Start Date], A.[End Date], T.Tool
        FROM Modelling AS A
	    INNER JOIN Tools AS T ON A.Tool_internal_id = T.Tool_internal_id
	    WHERE T.Tool LIKE '%{partialName}%'
        UNION
        SELECT A.Activity_internal_id, A.[Refers To], A.[Responsible Institute], A.[Responsible Person], A.Technique, A.[Start Date], A.[End Date], T.Tool
        FROM Optimising AS A
	    INNER JOIN Tools AS T ON A.Tool_internal_id = T.Tool_internal_id
	    WHERE T.Tool LIKE '%{partialName}%'
        UNION
        SELECT A.Activity_internal_id, A.[Refers To], A.[Responsible Institute], A.[Responsible Person], A.Technique, A.[Start Date], A.[End Date], T.Tool
        FROM Processing AS A
	    INNER JOIN Tools AS T ON A.Tool_internal_id = T.Tool_internal_id
	    WHERE T.Tool LIKE '%{partialName}%';
        """
        return self.executeQuery(sql_command)

    def getActivitiesStartedAfter(self, date: str) -> DataFrame:
        sql_command = f"""
        SELECT A.Activity_internal_id, A.[Refers To], A.[Responsible Institute], A.[Responsible Person], A.Technique, A.[Start Date], A.[End Date], T.Tool
        FROM Acquisition AS A
	    INNER JOIN Tools AS T ON A.Tool_internal_id = T.Tool_internal_id
	    WHERE A.[Start Date] >= '{date}'
        UNION
        SELECT A.Activity_internal_id, A.[Refers To], A.[Responsible Institute], A.[Responsible Person], A.Technique, A.[Start Date], A.[End Date], T.Tool
        FROM Exporting AS A
	    INNER JOIN Tools AS T ON A.Tool_internal_id = T.Tool_internal_id
	    WHERE A.[Start Date] >= '{date}'
        UNION
        SELECT A.Activity_internal_id, A.[Refers To], A.[Responsible Institute], A.[Responsible Person], A.Technique, A.[Start Date], A.[End Date], T.Tool
        FROM Modelling AS A
	    INNER JOIN Tools AS T ON A.Tool_internal_id = T.Tool_internal_id
	    WHERE A.[Start Date] >= '{date}'
        UNION
        SELECT A.Activity_internal_id, A.[Refers To], A.[Responsible Institute], A.[Responsible Person], A.Technique, A.[Start Date], A.[End Date], T.Tool
        FROM Optimising AS A
	    INNER JOIN Tools AS T ON A.Tool_internal_id = T.Tool_internal_id
	    WHERE A.[Start Date] >= '{date}'
        UNION
        SELECT A.Activity_internal_id, A.[Refers To], A.[Responsible Institute], A.[Responsible Person], A.Technique, A.[Start Date], A.[End Date], T.Tool
        FROM Processing AS A
	    INNER JOIN Tools AS T ON A.Tool_internal_id = T.Tool_internal_id
	    WHERE A.[Start Date] >= '{date}';
        """
        return self.executeQuery(sql_command)

    def getActivitiesEndedBefore(self, date: str) -> DataFrame:
        sql_command = f"""
        SELECT A.Activity_internal_id, A.[Refers To], A.[Responsible Institute], A.[Responsible Person], A.Technique, A.[Start Date], A.[End Date], T.Tool
        FROM Acquisition AS A
	    INNER JOIN Tools AS T ON A.Tool_internal_id = T.Tool_internal_id
	    WHERE A.[End Date] <= '{date}'
        UNION
        SELECT A.Activity_internal_id, A.[Refers To], A.[Responsible Institute], A.[Responsible Person], A.Technique, A.[Start Date], A.[End Date], T.Tool
        FROM Exporting AS A
	    INNER JOIN Tools AS T ON A.Tool_internal_id = T.Tool_internal_id
	    WHERE A.[End Date] <= '{date}'
        UNION
        SELECT A.Activity_internal_id, A.[Refers To], A.[Responsible Institute], A.[Responsible Person], A.Technique, A.[Start Date], A.[End Date], T.Tool
        FROM Modelling AS A
	    INNER JOIN Tools AS T ON A.Tool_internal_id = T.Tool_internal_id
	    WHERE A.[End Date] <= '{date}'
        UNION
        SELECT A.Activity_internal_id, A.[Refers To], A.[Responsible Institute], A.[Responsible Person], A.Technique, A.[Start Date], A.[End Date], T.Tool
        FROM Optimising AS A
	    INNER JOIN Tools AS T ON A.Tool_internal_id = T.Tool_internal_id
	    WHERE A.[End Date] <= '{date}'
        UNION
        SELECT A.Activity_internal_id, A.[Refers To], A.[Responsible Institute], A.[Responsible Person], A.Technique, A.[Start Date], A.[End Date], T.Tool
        FROM Processing AS A
	    INNER JOIN Tools AS T ON A.Tool_internal_id = T.Tool_internal_id
	    WHERE A.[End Date] <= '{date}';
        """
        return self.executeQuery(sql_command)

    def getAcquisitionsByTechnique(self, partialName: str) -> DataFrame:
        sql_command = """
        SELECT A.Activity_internal_id, A.[Refers To], A.[Responsible Institute], A.[Responsible Person], A.Technique, A.[Start Date], A.[End Date], T.Tool
        FROM Acquisition AS A
	    INNER JOIN Tools AS T ON A.Tool_internal_id = T.Tool_internal_id
	    WHERE LOWER(A.Technique) LIKE '%{}%'
        ORDER BY A.[Refers To];
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
        return df.replace(np.nan, " ");

    def getById(self, id):
        person_query = "SELECT DISTINCT ?uri ?name ?id WHERE { ?object <https://schema.org/author> ?uri.  ?uri <https://schema.org/name> ?name.  ?uri <https://schema.org/identifier> ?id. ?uri <https://schema.org/identifier> '%s'. }" % id;
        object_query = "SELECT DISTINCT ?object ?id ?type ?title ?date ?owner ?place ?author ?author_name ?author_id WHERE { ?object <https://schema.org/identifier> ?id. ?object <https://schema.org/identifier> '%s'. ?object <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?type. ?object <https://schema.org/name> ?title. ?object <https://schema.org/copyrightHolder> ?owner. ?object <https://schema.org/spatial> ?place. OPTIONAL{ ?object <https://schema.org/dateCreated> ?date. } OPTIONAL{ ?object <https://schema.org/author> ?author. ?author <https://schema.org/name> ?author_name. ?author <https://schema.org/identifier> ?author_id.}}" % id;
        person_df = self.execute_sparql_query(person_query);
        object_df = self.execute_sparql_query(object_query);
        if len(object_df.index) > 0: # if objects exist return objects
            return object_df;
        return person_df; # otherwise persons exists or we return an empty dataframe

    def getAllPeople(self):
        query = """
            SELECT DISTINCT ?author_id ?author_name
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
            OPTIONAL{ ?object <https://schema.org/dateCreated> ?date. }
            FILTER CONTAINS(?author_id, '%s')}
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
            tools = [];
            if row["Tool"] != '':
                tools = [s.strip() for s in row["Tool"].split("🤪")] if "🤪" in row["Tool"] else [row["Tool"]];
            if "Acquisition" in row["Activity_internal_id"]:
                activity = Acquisition(
                    cho,
                    row["Responsible Institute"],
                    row["Responsible Person"],
                    row["Start Date"],
                    row["End Date"],
                    row["Technique"],
                    tools,
                )
                activities.append(activity)
            elif "Processing" in row["Activity_internal_id"]:
                activity = Processing(
                    cho,
                    row["Responsible Institute"],
                    row["Responsible Person"],
                    row["Start Date"],
                    row["End Date"],
                    tools
                )
                activities.append(activity)
            elif "Modelling" in row["Activity_internal_id"]:
                activity = Modelling(
                    cho,
                    row["Responsible Institute"],
                    row["Responsible Person"],
                    row["Start Date"],
                    row["End Date"],
                    tools
                )
                activities.append(activity)
            elif "Optimising" in row["Activity_internal_id"]:
                activity = Optimising(
                    cho,
                    row["Responsible Institute"],
                    row["Responsible Person"],
                    row["Start Date"],
                    row["End Date"],
                    tools
                )
                activities.append(activity)
            elif "Exporting" in row["Activity_internal_id"]:
                activity = Exporting(
                    cho,
                    row["Responsible Institute"],
                    row["Responsible Person"],
                    row["Start Date"],
                    row["End Date"],
                    tools
                )
                activities.append(activity)
        
        return activities

    # helper method to reduce code clutter
    def createObjectList(self, cho_df): # Hubert
        cho_list = [];
        for idx, row in cho_df.iterrows():
            author_list = []
            if row["author_id"] != " " and row["author_name"] != " ":
                author_list.append(Person(row["author_id"], row["author_name"]))
            if "Nautical_chart" in row["type"]:
                obj = NauticalChart(row["id"], row["title"], row["date"], author_list, row["owner"], row["place"]);
                cho_list.append(obj)
            elif "Manuscript_plate" in row["type"]:
                obj = ManuscriptPlate(row["id"], row["title"], row["date"], author_list, row["owner"], row["place"]);
                cho_list.append(obj)
            elif "Manuscript_volume" in row["type"]:
                obj = ManuscriptVolume(row["id"], row["title"], row["date"], author_list, row["owner"], row["place"]);
                cho_list.append(obj)
            elif "Printed_volume" in row["type"]:
                obj = PrintedVolume(row["id"], row["title"], row["date"], author_list, row["owner"], row["place"]);
                cho_list.append(obj)
            elif "Printed_material" in row["type"]:
                obj = PrintedMaterial(row["id"], row["title"], row["date"], author_list, row["owner"], row["place"]);
                cho_list.append(obj)
            elif "Herbarium" in row["type"]:
                obj = Herbarium(row["id"], row["title"], row["date"], author_list, row["owner"], row["place"]);
                cho_list.append(obj)
            elif "Specimen" in row["type"]:
                obj = Specimen(row["id"], row["title"], row["date"], author_list, row["owner"], row["place"]);
                cho_list.append(obj)
            elif "Painting" in row["type"]:
                obj = Painting(row["id"], row["title"], row["date"], author_list, row["owner"], row["place"]);
                cho_list.append(obj)
            elif "Model" in row["type"]:
                obj = Model(row["id"], row["title"], row["date"], author_list, row["owner"], row["place"]);
                cho_list.append(obj)
            elif "Map" in row["type"]:
                obj = Map(row["id"], row["title"], row["date"], author_list, row["owner"], row["place"]);
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
        
        if 'name' in df.columns and 'id' in df.columns: 
            return Person(df.iloc[0]["id"], df.iloc[0]["name"])
        
        return None

    def getAllPeople(self):
        person_list = [];
        if len(self.metadataQuery) > 0:
            person_df = pd.DataFrame();
            new_person_df_list = [];
            for handler in self.metadataQuery:
                new_person_df = handler.getAllPeople();
                new_person_df_list.append(new_person_df);
            
            person_df = new_person_df_list[0];
            for d in new_person_df_list[1:]:
                person_df = person_df.merge(d, on=['author_id'], how='inner').drop_duplicates(subset=['author_id'], keep='first', inplace=True, ignore_index=True);
            
            for idx, row in person_df.iterrows():
                if row["author_id"] != " " and row["author_name"] != " ":
                    person = Person(row["author_id"], row["author_name"]);
                    person_list.append(person);
        return person_list;
    
    def getAllCulturalHeritageObjects(self) -> list[CulturalHeritageObject]:  # Iheb
       cho_list = []
       if len(self.metadataQuery) > 0:
        cho_df = pd.DataFrame();
        new_object_df_list = [];
        for handler in self.metadataQuery:
            new_object_df = handler.getAllCulturalHeritageObjects();
            new_object_df_list.append(new_object_df);
        
        cho_df = new_object_df_list[0];
        for d in new_object_df_list[1:]:
            cho_df = cho_df.merge(d, on=['id'], how='inner').drop_duplicates(subset=['id'], keep='first', inplace=True, ignore_index=True);

        cho_list =  self.createObjectList(cho_df)
       return cho_list

    def getAuthorsOfCulturalHeritageObject(self, objectId: str) -> list[Person]:
        author_list = []
        if len(self.metadataQuery) > 0:
            author_df = pd.DataFrame();
            new_person_df_list = [];
            for handler in self.metadataQuery:
                new_person_df = handler.getAuthorsOfCulturalHeritageObject(objectId);
                new_person_df_list.append(new_person_df);
            
            author_df = new_person_df_list[0];
            for d in new_person_df_list[1:]:
                author_df = author_df.merge(d, on=['author_id'], how='inner').drop_duplicates(subset=['author_id'], keep='first', inplace=True, ignore_index=True);

            for idx, row in author_df.iterrows():
                if row["author_id"] != " " and row["author_name"] != " ":
                    person = Person(row["author_id"], row["author_name"])
                    author_list.append(person)
        return author_list

    def getCulturalHeritageObjectsAuthoredBy(self, AuthorId: str) -> list[CulturalHeritageObject]:  # Iheb
       cho_list = []
       if len(self.metadataQuery) > 0:
        cho_df = pd.DataFrame();
        new_object_df_list = [];
        for handler in self.metadataQuery:
            new_object_df = handler.getCulturalHeritageObjectsAuthoredBy(AuthorId);
            new_object_df_list.append(new_object_df);
        
        cho_df = new_object_df_list[0];
        for d in new_object_df_list[1:]:
            cho_df = cho_df.merge(d, on=['id'], how='inner').drop_duplicates(subset=['id'], keep='first', inplace=True, ignore_index=True);

        cho_list =  self.createObjectList(cho_df)
        return cho_list

    def getAllActivities(self): # Giorgia
        activities = []
        if len(self.processQuery) > 0:
            activities_df = pd.DataFrame();
            new_activities_df_list = [];
            for handler in self.processQuery:
                new_activities_df = handler.getAllActivities();
                new_activities_df_list.append(new_activities_df);

            activities_df = new_activities_df_list[0];
            for d in new_activities_df_list[1:]:
                activities_df = activities_df.merge(d, on=['Activity_internal_id'], how='inner').drop_duplicates(subset=['Activity_internal_id'], keep='first', inplace=True, ignore_index=True);

            activities = self.createActivityList(activities_df);
        return activities;
    
    def getActivitiesByResponsibleInstitution(self, partialName: str) -> list[Activity]:
        activities = []  
        if len(self.processQuery) > 0:  # check handlers 
            activities_df = pd.DataFrame();
            new_activities_df_list = [];
            for handler in self.processQuery:
                new_activities_df = handler.getActivitiesByResponsibleInstitution(partialName);
                new_activities_df_list.append(new_activities_df);
            
            activities_df = new_activities_df_list[0];
            for d in new_activities_df_list[1:]:
                activities_df = activities_df.merge(d, on=['Activity_internal_id'], how='inner').drop_duplicates(subset=['Activity_internal_id'], keep='first', inplace=True, ignore_index=True);
            
            activities = self.createActivityList(activities_df) 
        return activities  

    def getActivitiesByResponsiblePerson(self, partialName: str) -> list[Activity]:  # Giorgia
        activities = []  
        if len(self.processQuery) > 0:  #check for handlers 
            activities_df = pd.DataFrame();
            new_activities_df_list = [];
            for handler in self.processQuery:
                new_activities_df = handler.getActivitiesByResponsiblePerson(partialName);
                new_activities_df_list.append(new_activities_df);
            
            activities_df = new_activities_df_list[0];
            for d in new_activities_df_list[1:]:
                activities_df = activities_df.merge(d, on=['Activity_internal_id'], how='inner').drop_duplicates(subset=['Activity_internal_id'], keep='first', inplace=True, ignore_index=True);
            
            activities = self.createActivityList(activities_df) 
        return activities  

    def getActivitiesUsingTool(self, partial_name: str):  # Giorgia
        activities = []
        if len(self.processQuery) > 0:
            activities_df = pd.DataFrame();
            new_activities_df_list = [];
            for handler in self.processQuery:
                new_activities_df = handler.getActivitiesUsingTool(partial_name);
                new_activities_df_list.append(new_activities_df);
            
            activities_df = new_activities_df_list[0];
            for d in new_activities_df_list[1:]:
                activities_df = activities_df.merge(d, on=['Activity_internal_id'], how='inner').drop_duplicates(subset=['Activity_internal_id'], keep='first', inplace=True, ignore_index=True);
            
            activities = self.createActivityList(activities_df);
        return activities;

    def getActivitiesStartedAfter(self, date: str) -> list[Activity]:
        activities = []  
        if len(self.processQuery) > 0:  
            activities_df = pd.DataFrame();
            new_activities_df_list = [];
            for handler in self.processQuery:
                new_activities_df = handler.getActivitiesStartedAfter(date) ;
                new_activities_df_list.append(new_activities_df);
            
            activities_df = new_activities_df_list[0];
            for d in new_activities_df_list[1:]:
                activities_df = activities_df.merge(d, on=['Activity_internal_id'], how='inner').drop_duplicates(subset=['Activity_internal_id'], keep='first', inplace=True, ignore_index=True);
            
            activities = self.createActivityList(activities_df) 
        return activities  
        
    def getActivitiesEndedBefore(self, date: str) -> list[Activity]:
        activities = []  
        if len(self.processQuery) > 0:  
            activities_df = pd.DataFrame();
            new_activities_df_list = [];
            for handler in self.processQuery:
                new_activities_df = handler.getActivitiesEndedBefore(date) ;
                new_activities_df_list.append(new_activities_df);
            
            activities_df = new_activities_df_list[0];
            for d in new_activities_df_list[1:]:
                activities_df = activities_df.merge(d, on=['Activity_internal_id'], how='inner').drop_duplicates(subset=['Activity_internal_id'], keep='first', inplace=True, ignore_index=True);
            
            activities = self.createActivityList(activities_df) 
        return activities

    def getAcquisitionsByTechnique(self, technique: str) -> list[Acquisition]: #Giorgia
        activities = []  
        if len(self.processQuery) > 0:  
            activities_df = pd.DataFrame();
            new_activities_df_list = [];
            for handler in self.processQuery:
                new_activities_df = handler.getAcquisitionsByTechnique(technique);
                new_activities_df_list.append(new_activities_df);
            
            activities_df = new_activities_df_list[0];
            for d in new_activities_df_list[1:]:
                activities_df = activities_df.merge(d, on=['Activity_internal_id'], how='inner').drop_duplicates(subset=['Activity_internal_id'], keep='first', inplace=True, ignore_index=True);
            
            activities = self.createActivityList(activities_df) 
        return activities 

class AdvancedMashup(BasicMashup):
    def getActivitiesOnObjectsAuthoredBy(self, personId: str) -> list[Activity]: #Giorgia
        activities = []  
        
        cho_list = self.getAllCulturalHeritageObjects()
        filt_cho = []
        for item in cho_list:
            person = item.getAuthors()
            if person:
                person_id = person[0].id
                if str(personId) in str(person_id) and item not in filt_cho:
                    filt_cho.append(item)
       
        activities_list = self.getAllActivities()
        for item in activities_list:
            for cho in filt_cho: 
                if item.refersTo_cho.id == cho.id:
                    activities.append(item)
        return activities 
    
    def getObjectsHandledByResponsiblePerson(self, partialName: str) -> list[CulturalHeritageObject]:  #giorgia
        objects = []
        if len(self.processQuery) > 0:
            activities = self.getActivitiesByResponsiblePerson(partialName)
            if len(self.metadataQuery) > 0:
                object_list = self.getAllCulturalHeritageObjects()
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
            activities = self.getActivitiesByResponsibleInstitution(partialName)
            if len(self.metadataQuery) > 0:
                object_list = self.getAllCulturalHeritageObjects()
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
        author_ids = []
        if len(self.processQuery) > 0:
            activities_before = self.getActivitiesEndedBefore(end)
            activities_after = self.getActivitiesStartedAfter(start)
            
            acquisitions_before_set = {activity for activity in activities_before if isinstance(activity, Acquisition)}
            acquisitions_after_set = {activity for activity in activities_after if isinstance(activity, Acquisition)}
        
            activity_list = []
            filtered_ids = []

            for acquisition in acquisitions_before_set:
                filtered_ids.append(acquisition.refersTo_cho.id)

            for acquisition in acquisitions_after_set:
                if acquisition.refersTo_cho.id in filtered_ids:
                    activity_list.append(acquisition)
            
            for activity in activity_list:
                if activity.refersTo_cho.hasAuthor:
                    author = activity.refersTo_cho.hasAuthor[0] 
                    author_id = author.id
                    if author_id not in author_ids and author_id != " ":
                        author_ids.append(author_id)
                        authors.append(author)
        return authors
