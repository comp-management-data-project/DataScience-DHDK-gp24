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
        pass


class QueryHandler(Handler): 
    def __init__(self, dbPathOrUrl=""):  # Provide a default value for dbPathOrUrl
        super().__init__()
        self.dbPathOrUrl = dbPathOrUrl

    def getById(self, id: str):
        return None
    
  # the following implementation of getById might be helpful for MetadataQueryHandler
  # if so, add it to that class, otherwise just remove it  

    # idDataFrame = None

    # def set_id_dataframe(self, dataframe):
    #     self.idDataFrame = dataframe

    # def getById(self, id: str):
    #     if self.idDataFrame is None:
    #         raise ValueError("idDataFrame is not set. Call set_id_dataframe first.")
    #     id_value = self.idDataFrame["id"]
    #     return id_value
    


# Class to upload data from JSON to SQLite database 

class ProcessDataUploadHandler(Handler):    # Lucrezia
    def __init__(self, db_name=""):
        super().__init__()
        self.db_name = db_name

    def process_data(self, json_data):
        file_path = os.path.join("resources", "process.json")
        with open(file_path, "r") as file:
            json_data = json.load(file)

        object_id_mapping = self.map_object_ids(json_data)
        activity_dfs, tools_data = self.create_dataframes(json_data, object_id_mapping)
        return activity_dfs, tools_data

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


    def pushDataToDb(self, activity_dfs, tools_df):
        try:
            with connect(self.db_name) as conn:
                for activity_type, df in activity_dfs.items():
                    df.to_sql(activity_type.capitalize(), conn, if_exists='replace', index=False)
                tools_df.to_sql('Tools', conn, if_exists='replace', index=False)
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
    def __init__(self, dbPathOrUrl):
        self.dbPathOrUrl = "";

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

class ProcessDataQueryHandler(Handler):        # Lucrezia
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
    


# Class to interact with Blazegraph database

class MetadataQueryHandler(QueryHandler):

    idDataFrame = None

    def set_id_dataframe(self, dataframe):
        self.idDataFrame = dataframe

    def getById(self, id):
        person_query = "SELECT DISTINCT ?uri ?name ?id WHERE { ?object <https://schema.org/author> ?uri.  ?uri <https://schema.org/name> ?name.  ?uri <https://schema.org/identifier> ?id. ?uri <https://schema.org/identifier> '%s'. }" % id;
        object_query = "SELECT DISTINCT ?object ?id ?type ?title ?date ?owner ?place ?author ?author_name ?author_id WHERE { ?object <https://schema.org/identifier> ?id.  ?object <https://schema.org/identifier> '%s'. ?object <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?type. ?object <https://schema.org/name> ?title. ?object <https://schema.org/dateCreated> ?date. ?object <https://schema.org/copyrightHolder> ?owner. ?object <https://schema.org/spatial> ?place. ?object <https://schema.org/author> ?author. ?author <https://schema.org/name> ?author_name. ?author <https://schema.org/identifier> ?author_id.}" % id;
        person_df = self.execute_query(person_query);
        object_df = self.execute_query(object_query);
        if len(object_df.index) > 0: # if objects exist return objects
            return object_df;
        return person_df; # otherwise persons exists or we return an empty dataframe
    
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

    def getAllPeople(self):
        query = """
            SELECT DISTINCT ?personId ?personName
            WHERE {
                ?item schema:author ?person .
                ?person schema:identifier ?personId .
                ?person schema:name ?personName .
            }
            """
        return self.execute_sparql_query(query)

    def getAllCulturalHeritageObjects(self):
        query = """
            SELECT DISTINCT ?object ?objectName #add all (type, id, )
            WHERE {
                ?object a ?type ;
                schema:name ?objectName .
                FILTER(REGEX(STR(?type), "http://dbpedia.org/resource"))
            }
            """
        return self.execute_sparql_query(query)

    def getAuthorsOfCulturalHeritageObject(self, object_id: str) -> pd.DataFrame:
        query = """
            SELECT ?authorId ?authorName
            WHERE {
            <%s> schema:author ?authorId . 
            ?authorId schema:name ?authorName .
            }
        """ % object_id
        return self.execute_sparql_query(query)

    def getCulturalHeritageObjectsAuthoredBy(self, personId): #Giorgia
        query = """
          SELECT ?objectId ?objectName
          WHERE {
             ?objectId schema:author %s .
             ?objectId schema:name ?objectName .
             FILTER (?author = "%s"^^xsd:string)
            }
            """ % (personId, personId)
        return self.execute_sparql_query(query)




# ======================================================== #

# Mashup classes


"""
    WHICH ONE OF THE TWO __init__ ARE WE KEEPING ???
    one of them needs to be deleted 
"""

class BasicMashup(object):  #Hubert
    def __init__(self):
        self.metadataQuery = [];
        self.processQuery = [];

    def __init__(self, metadataQuery, processQuery):
        self.metadataQuery = metadataQuery;
        self.processQuery = processQuery;
    
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

    # helper method to reduce code clutter # Hubert
    def createActivityList(self, df): # Hubert 
        activities = []
        
        # formatted version Lucrezia
        for idx, row in df.iterrows():
            if row["type"] == "acquisition":
                activity = Acquisition(
                    row["responsible_institute"],
                    row["responsible_person"],
                    row["tool"],
                    row["start_date"],
                    row["end_date"],
                    row["object_id"],
                    row["technique"]
                )
            elif "acquisition" in row["Activity_internal_id"]:
                activity = Acquisition(
                    CulturalHeritageObject(row["Activity_internal_id"].rsplit('-'), "", "", "", "", ""),
                    row["Responsible Institute"],
                    row["Responsible Person"],
                    row["Tool"],
                    row["Start Date"],
                    row["End Date"],
                    row["Technique"]
                )
                activities.append(activity)
            elif row["type"] == "processing":
                activity = Processing(
                    row["responsible_institute"],
                    row["responsible_person"],
                    row["tool"],
                    row["start_date"],
                    row["end_date"],
                    row["object_id"]
                )
            elif "processing" in row["Activity_internal_id"]:
                activity = Processing(
                    CulturalHeritageObject(row["activity_internal_id"].rsplit('-'), "", "", "", "", ""),
                    row["Responsible Institute"],
                    row["Responsible Person"],
                    row["Tool"],
                    row["Start Date"],
                    row["End Date"]
                )
                activities.append(activity)
            elif row["type"] == "modelling":
                activity = Modelling(
                    row["responsible_institute"],
                    row["responsible_person"],
                    row["tool"],
                    row["start_date"],
                    row["end_date"],
                    row["object_id"]
                )
            elif "modelling" in row["Activity_internal_id"]:
                activity = Modelling(
                    CulturalHeritageObject(row["activity_internal_id"].rsplit('-'), "", "", "", "", ""),
                    row["Responsible Institute"],
                    row["Responsible Person"],
                    row["Tool"],
                    row["Start Date"],
                    row["End Date"]
                )
                activities.append(activity)
            elif row["type"] == "optimising":
                activity = Optimising(
                    row["responsible_institute"],
                    row["responsible_person"],
                    row["tool"],
                    row["start_date"],
                    row["end_date"],
                    row["object_id"]
                )
            elif "optimising" in row["Activity_internal_id"]:
                activity = Optimising(
                    CulturalHeritageObject(row["activity_internal_id"].rsplit('-'), "", "", "", "", ""),
                    row["Responsible Institute"],
                    row["Responsible Person"],
                    row["Tool"],
                    row["Start Date"],
                    row["End Date"]
                )
                activities.append(activity)
            elif row["type"] == "exporting":
                activity = Exporting(
                    row["responsible_institute"],
                    row["responsible_person"],
                    row["tool"],
                    row["start_date"],
                    row["end_date"],
                    row["object_id"]
                )
            elif "exporting" in row["Activity_internal_id"]:
                activity = Exporting(
                    CulturalHeritageObject(row["activity_internal_id"].rsplit('-'), "", "", "", "", ""),
                    row["Responsible Institute"],
                    row["Responsible Person"],
                    row["Tool"],
                    row["Start Date"],
                    row["End Date"]
                )
                activities.append(activity)
        
        return activities

    # helper method to reduce code clutter
    # column names here might have to be changed, depending on your implementation of loading data to sparql
    def createObjectList(self, cho_df): # Hubert
        cho_list = [];
        for idx, row in cho_df.iterrows():
            if "Nautical_chart" in row["type"]:
                obj = NauticalChart();
                obj.id = row["id"];
                obj.title = row["title"];
                obj.date = row["date"];
                obj.owner = row["owner"];
                obj.place = row["place"];
                obj.author = [Person(row["author_id"], row["author_name"])];
                cho_list.append(obj)
            elif "Manuscript_plate" in row["type"]:
                obj = ManuscriptPlate();
                obj.id = row["id"];
                obj.title = row["title"];
                obj.date = row["date"];
                obj.owner = row["owner"];
                obj.place = row["place"];
                obj.author = [Person(row["author_id"], row["author_name"])];
                cho_list.append(obj)
            elif "Manuscript_volume" in row["type"]:
                obj = ManuscriptVolume();
                obj.id = row["id"];
                obj.title = row["title"];
                obj.date = row["date"];
                obj.owner = row["owner"];
                obj.place = row["place"];
                obj.author = [Person(row["author_id"], row["author_name"])];
                cho_list.append(obj)
            elif "Printed_volume" in row["type"]:
                obj = PrintedVolume();
                obj.id = row["id"];
                obj.title = row["title"];
                obj.date = row["date"];
                obj.owner = row["owner"];
                obj.place = row["place"];
                obj.author = [Person(row["author_id"], row["author_name"])];
                cho_list.append(obj)
            elif "Printed_material" in row["type"]:
                obj = PrintedMaterial();
                obj.id = row["id"];
                obj.title = row["title"];
                obj.date = row["date"];
                obj.owner = row["owner"];
                obj.place = row["place"];
                obj.author = [Person(row["author_id"], row["author_name"])];
                cho_list.append(obj)
            elif "Herbarium" in row["type"]:
                obj = Herbarium();
                obj.id = row["id"];
                obj.title = row["title"];
                obj.date = row["date"];
                obj.owner = row["owner"];
                obj.place = row["place"];
                obj.author = [Person(row["author_id"], row["author_name"])];
                cho_list.append(obj)
            elif "Specimen" in row["type"]:
                obj = Specimen();
                obj.id = row["id"];
                obj.title = row["title"];
                obj.date = row["date"];
                obj.owner = row["owner"];
                obj.place = row["place"];
                obj.author = [Person(row["author_id"], row["author_name"])];
                cho_list.append(obj)
            elif "Painting" in row["type"]:
                obj = Painting();
                obj.id = row["id"];
                obj.title = row["title"];
                obj.date = row["date"];
                obj.owner = row["owner"];
                obj.place = row["place"];
                obj.author = [Person(row["author_id"], row["author_name"])];
                cho_list.append(obj)
            elif "Model" in row["type"]:
                obj = Model();
                obj.id = row["id"];
                obj.title = row["title"];
                obj.date = row["date"];
                obj.owner = row["owner"];
                obj.place = row["place"];
                obj.author = [Person(row["author_id"], row["author_name"])];
                cho_list.append(obj)
            elif "Map" in row["type"]:
                obj = Map();
                obj.id = row["id"];
                obj.title = row["title"];
                obj.date = row["date"];
                obj.owner = row["owner"];
                obj.place = row["place"];
                obj.author = [Person(row["author_id"], row["author_name"])];
                cho_list.append(obj)
        return cho_list;

    """
    general sidenote: do filtering with SQL and SPARQL
    """
    
    """
    good try, however:
    - here we should implement getById in the MetadataQueryHandler I'm afraid, and run two queries, one checking for id of authors, another for id of objects
    - remember that this class has a list of MetadataQueryHandler objects, so to get the dataframe, use something like cho_df = self.metadataQuery[0].getById(id)
    - also beforehand check if there are any handlers actually in the list like if self.metadataQuery: 
    - then you'll have to check if the dataframe has a cultural heritage objects or a person
    """
    def getEntityById(self, cho_df, entity_id: str) -> IdentifiableEntity | None:  # Giorgia
        entity_list = []
        for entity in self.createObjectList(cho_df):
            if entity.id == entity_id:
                entity_list.append((entity))
        if entity_list:
            return entity_list  
        else: 
            return None

    """
    I've added my getAllPeople() function, as it's a nice start for all the other functions of this class
    """
    def getAllPeople(self):
        person_list = [];
        if len(self.metadataQuery) > 0:
            person_df = self.metadataQuery[0].getAllPeople();
            for idx, row in person_df.iterrows():
                person = Person(row["id"], row["name"]);
                person_list.append(person);
        return person_list;
    
    """
    alright so:
    - at the start, remember to initialise the list of returned objects as empty and writing the return statement
    - cho_df should be retrieved from a handler from this class' list of MetadataQueryHandlers, not passed in the constructor
    - eg. cho_df = self.metadataQuery[0].getAllCulturalHeritageObjects()
    - then you pass this cho_df as an argument for the createObjectList, so: cho_list = self.createObjectList(cho_df)
    - these two lines should be a part of an if statement checking if the list of metadata handlers is not empty: if self.metadataQuery:
    (take a look at getAllActivities)
    """
    def getAllCulturalHeritageObjects(self) -> list[CulturalHeritageObject]:  # Iheb
       cho_list = []
       if len(self.metadataQuery) > 0:
        cho_df = self.metadataQuery[0].getAllCulturalHeritageObjects()
        cho_list =  self.createObjectList(cho_df)
       return cho_list

    """
    this is basically getAllPeople, with one line changed:
    - call getAuthorsOfCulturalHeritageObject(objectId) instead of getAllPeople
    this method is supposed to return a list of Person objects, nice to see the list comprehension, but it would return a list of one author name as a string
    """

    def getAuthorsOfCulturalHeritageObject(self, objectId: str) -> list[Person]:
        author_list = []
        if len(self.metadataQuery) > 0:
            author_df = self.metadataQuery[0].getAuthorsOfCulturalHeritageObject(objectId)
            for idx, row in author_df.iterrows():
                person = Person(row["id"], row["name"])
                author_list.append(person)
        return author_list



    """
    this is basically getAllCulturalHeritageObjects with one line changed:
    - call getCulturalHeritageObjectsAuthoredBy(AuthorId) instead of getAllCulturalHeritageObjects()
    """
    def getCulturalHeritageObjectsAuthoredBy(self, AuthorId: str) -> list[CulturalHeritageObject]:  # Iheb
       cho_list = []
       if len(self.metadataQuery) > 0:
        cho_df = self.metadataQuery[0].getCulturalHeritageObjectsAuthoredBy(AuthorId)
        cho_list =  self.createObjectList(cho_df)
        return cho_list



    """
    activities methods are basically all the same so I think it's only necessary to describe one:
    - we initialise a list of activities as an empty list and write a return statement at the end of the function
    - this class has an attribute processQuery, which is acting as a list of ProcessDataQueryHandler objects
    - so, don't pass the dataframes as attributes in this function, check if there are handlers in that list
    - if so, call an appropriate self.processQuery[0] method, like getAllActivities here (the functions from this class all have a corresponding function with the same name in the QueryHandlers)
    - then call self.CreateActivityList() using the dataframe you got from the previous step
    """
    def getAllActivities(self): # Giorgia
        activities = []
        if len(self.processQuery) > 0:
            activities_df = self.processQuery[0].getAllActivities();
            activities = self.createActivityList(activities_df);
        return activities;

    """
    basically the same as getAllActivities, just use a different method from ProcessDataQueryHandler
    use filtering in SQL
    """
    #like this????? I
    def getActivitiesByResponsibleInstitution(self, partialName: str) -> list[Activity]:
        activities = []  
        if len(self.processQuery) > 0:  # check handlers 
            activities_df = self.processQuery[0].getResponsibleInstitute(partialName)  
            activities = self.createActivityList(activities_df) 
        return activities  

    """
    basically the same as getAllActivities, just use a different method from ProcessDataQueryHandler
    use filtering in SQL
    """
    def getActivitiesByResponsiblePerson(self, df, partialName: str) -> list[Activity]:  # Giorgia
        activities = self.createActivityList(df)
        resp_pers_filtered_activities = [activity for activity in activities if partialName in activity.responsible_person]
        return resp_pers_filtered_activities

    """
    basically the same as getAllActivities, just use a different method from ProcessDataQueryHandler
    use filtering in SQL
    """
    def getActivitiesUsingTool(self, df, partial_name: str):  # Giorgia
        activities = self.createActivityList(df)
        filtered_activities = [activity for activity in activities if partial_name in Activity.getTools()]
        return filtered_activities

    """
    basically the same as getAllActivities, just use a different method from ProcessDataQueryHandler
    use filtering in SQL
    """
    def getActivitiesStartedAfter(self, df, date: str) -> list[Activity]:  # Giorgia
        start_date = datetime.strptime(date, "%Y-%m-%d")   # from date to a datetime obj (I am unsure about the "%Y-%m-%d")

        activities = self.createActivityList(df) #activities list from df
        after_date_filtered_activities = [activity for activity in activities if datetime.strptime(activity.start_date, "%Y-%m-%d") >= start_date] #grab only activities after specific date 
        
        return after_date_filtered_activities

    """
    basically the same as getAllActivities, just use a different method from ProcessDataQueryHandler
    use filtering in SQL
    """
    def getActivitiesEndedBefore(self, df, date: str) -> list[Activity]: #Giorgia
        end_date = datetime.strptime(date, "%Y-%m-%d")   # from date to a datetime obj

        activities = self.createActivityList(df) #activities list from df
        before_date_filtered_activities = [activity for activity in activities if datetime.strptime(activity.end_date, "%Y-%m-%d") <= end_date] #grab only activities before specific date 
        
        return before_date_filtered_activities

    """
    basically the same as getAllActivities, just use a different method from ProcessDataQueryHandler
    use filtering in SQL
    """
    def getAcquisitionsByTechnique(self, df, technique: str) -> list[Acquisition]: # Giorgia
        activities = self.createActivityList(df)
        matching_acquisitions = [activity for activity in activities if isinstance(activity, Acquisition) and technique in activity.technique] #only matching activities and technique: is activity an instance of class Acquisition and is technique a substring of activity.technique
        return matching_acquisitions

class AdvancedMashup(BasicMashup):
    #I've written two things; I don't really know if either of them are correct to be honest 
    def getActivitiesOnObjectsAuthoredBy(self, personId: str) -> list[Activity]: #fairly sure it's wrong
        activities = []
        if len(self.processQuery) > 0:  # Check for handlers
            activities_df = self.processQuery[0].getActivitiesOnObjectsAuthoredBy(personId)  # Activities with the same author

            for idx, row in activities_df.iterrows():
                # Determine the class based on the row information (e.g., type of activity)
                if row['ActivityType'] == 'Acquisition':
                    activity = Acquisition(
                        refers_to_cho=self.getEntityById(row['id']),  
                        institute=row['Responsible Institute'],
                        person=row['Responsible Person'],
                        start=row['Start Date'],
                        end=row['End Date'],
                        technique=row['Technique'],
                        
                    )
                # elif: 
                    
                # all the other types in activities
                else:
                    activity = Activity(
                        row['Activity_internal_id'],
                        row['Refers To'],
                        row['Responsible Institute'],
                        row['Responsible Person'],
                        row['Technique'],
                        row['Start Date'],
                        row['End Date']
                    )

                activities.append(activity)

        return activities

    """
    option 2
        def getActivitiesOnObjectsAuthoredBy(self, personId: str) -> list[Activity]:
                activities_on_objects = []
                
                if self.metadataQuery:
                    # cho with same author 
                    cho_df = self.metadataQuery[0].getCulturalHeritageObjectsAuthoredBy(personId)
                    cho_list = self.createObjectList(cho_df)
                    
                    if self.processQuery:
                        # activities for each cho
                        for cho in cho_list:
                            activities_df = self.processQuery[0].getActivitiesByObjectId(cho.getId())
                            activities_on_objects.extend(self.createActivityList(activities_df))
                
                return activities_on_objects
    """

    def getObjectsHandledByResponsiblePerson(self, partialName: str) -> list[CulturalHeritageObject]:  #
        pass

    def getObjectsHandledByResponsibleInstitution(self, partialName: str) -> list[CulturalHeritageObject]:  # Iheb
        pass

    def getAuthorsOfObjectsAcquiredInTimeFrame(self, start: str, end: str) -> list[Person]:  # Iheb
        pass

