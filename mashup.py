# imports for relational db
from sqlite3 import connect;
from json import load

# imports for graph db
from rdflib import RDF, Graph, URIRef, Literal;
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore;
import SPARQLWrapper;

# imports for both
import pandas as pd
from pandas import DataFrame, Series


# mashups #Hubert
class BasicMashup:
    def __init__(self):
        self.metadataQuery = [];
        self.processQuery = [];
        Entityid = pd.read_csv("meta.csv")
        Entity = []
        for id in Entityid:
            if id.Entityid == Entityid:
                return Entity
            else:
                return None
    def getAllPeople():
        Allpeople = []
        People = pd.read_csv("meta.csv", keep_default_na=False, dtype= {
            "id": "string", "Type":"string", "Title":"String","Date":"String", "Author":"String", "Owner":"String", "Place":"String"})
        PeopleNames = People["Author"]
        for i in PeopleNames:
            Allpeople.append(PeopleNames)
        return Allpeople

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
