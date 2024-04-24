# from sqlite3 import connect
# from pandas import read_sql, DataFrame, concat, read_csv, Series, merge
# from utils.paths import RDF_DB_URL, SQL_DB_URL
# from rdflib import Graph, Namespace
# from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
# from sparql_dataframe import get
# from clean_str import remove_special_chars
# from json import load
# from utils.CreateGraph import create_Graph
# from urllib.parse import urlparse

# imports for relational db
from sqlite3 import connect;
import json;

# imports for graph db
from rdflib import RDF, Graph, URIRef, Literal;
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore;
import SPARQLWrapper;

# imports for both
import pandas as pd

# data classes
class Activity(): # relation = Activity "refersTo" cho
    def __init__(self, id, institute, person, tool, start, end):
        self.institute = institute 
        self.person = person
        self.tool = set() 

        if tool is not None:
            if isinstance(tool, str):
                self.tool.add(tool)  # If it's a single string (one tool), add it to the set
            elif isinstance(tool, set):
                self.tool.update(tool)  # If there are multiple tools, add all to the set

        self.start = start
        self.end = end

    def getResponsibleInstitute(self):
        return self.institute
    
    def getResponsiblePerson(self):
        if self.person:
            return self.person
        else:
            return None
    
    def getTools(self): # getTool has arity zero or more [0..*]
        return self.tool
    
    def getStartDate(self):
        if self.start:
            return self.start
        else:
            return None

    def getEndDate(self):
        if self.end:
            return self.end
        else:
            return None

    def refersTo(self):
        return self.CulturalHeritageObject
    


class Acquisition(Activity):
    def __init__(self, id, institute, person, tool, start, end, technique: str):
        super().__init__(id, institute, person, tool, start, end)
        self.technique = technique

# handlers
class Handler:
    def __init__(self):
        self.dbPathOrUrl = "";

    def __init__(self, dbPathOrUrl): # overrides the first init but this class itself is never created so oh well
        self.dbPathOrUrl = dbPathOrUrl;

    def getDbPathOrUrl(self):
        return self.dbPathOrUrl;

    def setDbPathOrUrl(self, pathOrUrl):
        self.dbPathOrUrl = pathOrUrl;
        if self.dbPathOrUrl == pathOrUrl:
            return True;
        return False;

class UploadHandler(Handler):
    def __init__(self):
        self.dbPathOrUrl = "";

    def __init__(self, dbPathOrUrl): # overrides the first init but this class itself is never created so oh well
        self.dbPathOrUrl = dbPathOrUrl;

    def pushDataToDb(path):
        return False;

class QueryHandler(Handler):
    def __init__(self):
        self.dbPathOrUrl = "";

    def __init__(self, dbPathOrUrl): # overrides the first init but this class itself is never created so oh well
        self.dbPathOrUrl = dbPathOrUrl;

    def getById(self, id):
        return None; # never accessed here and overridden in child class

class MetadataQueryHandler(QueryHandler):
    def __init__(self):
        self.dbPathOrUrl = "";

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

class ProcessDataQueryHandler(QueryHandler):
    def __init__(self):
        self.dbPathOrUrl = "";
    
    # helper method to reduce code clutter
    def executeQuery(self, sql_command): 
        connection = connect(self.dbPathOrUrl);
        cursor = connection.cursor();
        cursor.execute(sql_command);
        df = pd.DataFrame(cursor.fetchall(), columns = [description[0] for description in cursor.description]);
        connection.close();
        return df;

    # UML methods go here

# mashups
class BasicMashup:
    def __init__(self):
        self.metadataQuery = [];
        self.processQuery = [];

    def __init__(self, metadataQuery, processQuery):
        self.metadataQuery = metadataQuery;
        self.processQuery = processQuery;

    # helper method to reduce code clutter
    # column names here might have to be changed, depending on your implementation of loading data to sql
    def createActivityList(self, df): 
        activities = [];
        for idx, row in df.iterrows():
            if row["type"] == "acquisition":
                activity = Acquisition(row["responsible_institute"], row["responsible_person"], row["tool"], row["start_date"], row["end_date"], row["object_id"], row["technique"]);
                activities.append(activity);
            elif row["type"] == "processing":
                activity = Processing(row["responsible_institute"], row["responsible_person"], row["tool"], row["start_date"], row["end_date"], row["object_id"]);
                activities.append(activity);
            elif row["type"] == "modelling":
                activity = Modelling(row["responsible_institute"], row["responsible_person"], row["tool"], row["start_date"], row["end_date"], row["object_id"]);
                activities.append(activity);
            elif row["type"] == "optimising":
                activity = Optimising(row["responsible_institute"], row["responsible_person"], row["tool"], row["start_date"], row["end_date"], row["object_id"]);
                activities.append(activity);
            elif row["type"] == "exporting":
                activity = Exporting(row["responsible_institute"], row["responsible_person"], row["tool"], row["start_date"], row["end_date"], row["object_id"]);
                activities.append(activity);
        return activities;

    # helper method to reduce code clutter
    # column names here might have to be changed, depending on your implementation of loading data to sparql
    def createObjectList(self, cho_df): 
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