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
import impl, query, upload;

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
    
    def cleanMetadataHandlers(self):
        self.metadataQuery = [];
        return True;

    def cleanProcessHandlers(self):
        self.processQuery = [];
        return True;

    def addMetadataHandler(self, handler):
        self.metadataQuery.append(handler);
        return True;

    def addProcessHandler(self, handler):
        self.processQuery.append(handler);
        return True;

    # helper method to reduce code clutter
    # column names here might have to be changed, depending on your implementation of loading data to sql
    def createActivityList(self, df): 
        activities = [];
        for idx, row in df.iterrows():
            if row["Activity_internal_id"].contains("acquisition"):
                activity = impl.Acquisition(impl.CulturalHeritageObject(row["activity_internal_id"].rsplit('-').lstrip("0"), "", "", "", "", ""), row["Responsible Institute"], row["Responsible Person"], row["Tool"], row["Start Date"], row["End Date"], row["Technique"]);
                activities.append(activity);
            if row["Activity_internal_id"].contains("processing"):
                activity = impl.Processing(impl.CulturalHeritageObject(row["activity_internal_id"].rsplit('-').lstrip("0"), "", "", "", "", ""), row["Responsible Institute"], row["Responsible Person"], row["Tool"], row["Start Date"], row["End Date"]);
                activities.append(activity);
            if row["Activity_internal_id"].contains("modelling"):
                activity = impl.Modelling(impl.CulturalHeritageObject(row["activity_internal_id"].rsplit('-').lstrip("0"), "", "", "", "", ""), row["Responsible Institute"], row["Responsible Person"], row["Tool"], row["Start Date"], row["End Date"]);
                activities.append(activity);
            if row["Activity_internal_id"].contains("optimising"):
                activity = impl.Optimising(impl.CulturalHeritageObject(row["activity_internal_id"].rsplit('-').lstrip("0"), "", "", "", "", ""), row["Responsible Institute"], row["Responsible Person"], row["Tool"], row["Start Date"], row["End Date"]);
                activities.append(activity);
            if row["Activity_internal_id"].contains("exporting"):
                activity = impl.Exporting(impl.CulturalHeritageObject(row["activity_internal_id"].rsplit('-').lstrip("0"), "", "", "", "", ""), row["Responsible Institute"], row["Responsible Person"], row["Tool"], row["Start Date"], row["End Date"]);
                activities.append(activity);
        return activities;

    # helper method to reduce code clutter
    # column names here might have to be changed, depending on your implementation of loading data to sparql
    def createObjectList(self, cho_df): 
        cho_list = [];
        for idx, row in cho_df.iterrows():
            if "Nautical_chart" in row["type"]:
                obj = impl.NauticalChart();
                obj.id = row["id"];
                obj.title = row["title"];
                obj.date = row["date"];
                obj.owner = row["owner"];
                obj.place = row["place"];
                obj.author = [impl.Person(row["author_id"], row["author_name"])];
                cho_list.append(obj)
            elif "Manuscript_plate" in row["type"]:
                obj = impl.ManuscriptPlate();
                obj.id = row["id"];
                obj.title = row["title"];
                obj.date = row["date"];
                obj.owner = row["owner"];
                obj.place = row["place"];
                obj.author = [impl.Person(row["author_id"], row["author_name"])];
                cho_list.append(obj)
            elif "Manuscript_volume" in row["type"]:
                obj = impl.ManuscriptVolume();
                obj.id = row["id"];
                obj.title = row["title"];
                obj.date = row["date"];
                obj.owner = row["owner"];
                obj.place = row["place"];
                obj.author = [impl.Person(row["author_id"], row["author_name"])];
                cho_list.append(obj)
            elif "Printed_volume" in row["type"]:
                obj = impl.PrintedVolume();
                obj.id = row["id"];
                obj.title = row["title"];
                obj.date = row["date"];
                obj.owner = row["owner"];
                obj.place = row["place"];
                obj.author = [impl.Person(row["author_id"], row["author_name"])];
                cho_list.append(obj)
            elif "Printed_material" in row["type"]:
                obj = impl.PrintedMaterial();
                obj.id = row["id"];
                obj.title = row["title"];
                obj.date = row["date"];
                obj.owner = row["owner"];
                obj.place = row["place"];
                obj.author = [impl.Person(row["author_id"], row["author_name"])];
                cho_list.append(obj)
            elif "Herbarium" in row["type"]:
                obj = impl.Herbarium();
                obj.id = row["id"];
                obj.title = row["title"];
                obj.date = row["date"];
                obj.owner = row["owner"];
                obj.place = row["place"];
                obj.author = [impl.Person(row["author_id"], row["author_name"])];
                cho_list.append(obj)
            elif "Specimen" in row["type"]:
                obj = impl.Specimen();
                obj.id = row["id"];
                obj.title = row["title"];
                obj.date = row["date"];
                obj.owner = row["owner"];
                obj.place = row["place"];
                obj.author = [impl.Person(row["author_id"], row["author_name"])];
                cho_list.append(obj)
            elif "Painting" in row["type"]:
                obj = impl.Painting();
                obj.id = row["id"];
                obj.title = row["title"];
                obj.date = row["date"];
                obj.owner = row["owner"];
                obj.place = row["place"];
                obj.author = [impl.Person(row["author_id"], row["author_name"])];
                cho_list.append(obj)
            elif "Model" in row["type"]:
                obj = impl.Model();
                obj.id = row["id"];
                obj.title = row["title"];
                obj.date = row["date"];
                obj.owner = row["owner"];
                obj.place = row["place"];
                obj.author = [impl.Person(row["author_id"], row["author_name"])];
                cho_list.append(obj)
            elif "Map" in row["type"]:
                obj = impl.Map();
                obj.id = row["id"];
                obj.title = row["title"];
                obj.date = row["date"];
                obj.owner = row["owner"];
                obj.place = row["place"];
                obj.author = [impl.Person(row["author_id"], row["author_name"])];
                cho_list.append(obj)
        return cho_list;
