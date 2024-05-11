import impl, query, upload

# imports for relational db
from sqlite3 import connect
from json import load

# imports for graph db
from rdflib import RDF, Graph, URIRef, Literal;
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore;
import SPARQLWrapper;

# imports for both
import pandas as pd
from pandas import DataFrame, Series



class BasicMashup(object):  #Hubert
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

    # helper method to reduce code clutter # Hubert
    # column names here might have to be changed, depending on your implementation of loading data to sql
    def createActivityList(self) -> list[impl.Activity]: # Hubert
        activities = []

        if self.metadataQuery:
            activities_data = self.metadataQuery.getAllActivities()
            for idx, row in activities_data.iterrows():
                activity_type = row["type"]
                cultural_object = impl.CulturalHeritageObject(row["object id"], "", "", "", "")
                institute = row[activity_type]["responsible institute"]
                person = row[activity_type]["responsible person"]
                tool = set(row[activity_type]["tool"])
                start = row[activity_type]["start date"]
                end = row[activity_type]["end date"]
                technique = row[activity_type]["technique"] if activity_type == "acquisition" else ""

                if activity_type == "acquisition":
                    activity = impl.Acquisition(cultural_object, institute, person, tool, start, end, technique)
                elif activity_type == "processing":
                    activity = impl.Processing(cultural_object, institute, person, tool, start, end)
                elif activity_type == "modelling":
                    activity = impl.Modelling(cultural_object, institute, person, tool, start, end)
                elif activity_type == "optimising":
                    activity = impl.Optimising(cultural_object, institute, person, tool, start, end)
                elif activity_type == "exporting":
                    activity = impl.Exporting(cultural_object, institute, person, tool, start, end)
                else:
                    continue  # Skip unknown activity types

                activities.append(activity)

        return activities

    # helper method to reduce code clutter
    # column names here might have to be changed, depending on your implementation of loading data to sparql
    def createObjectList(self, cho_df): # Hubert
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

    def cleanMetadataHandlers(self) -> bool:  #
        pass

    def cleanProcessHandlers(self) -> bool:  #
        pass

    def addMetadataHandler(self, handler: query.MetadataQueryHandler) -> bool:  #
        pass

    def getEntityById(self, entity_id: str) -> impl.IdentifiableEntity | None:  #
        pass

    def getAllCulturalHeritageObjects(self) -> list[impl.CulturalHeritageObject]:  #
        pass

    def getAuthorsOfCulturalHeritageObject(self, objectId: str) -> list[impl.Person]:  # 
        pass
    
    def getCulturalHeritageObjectsAuthoredBy(self, AuthorId: str) -> list[impl.CulturalHeritageObject]:  #
        pass

    def getAllActivities(self) -> list[impl.Activity]: # Lucrezia
        activities = []
        for processGrinder in self.processQuery:
            df_process = processGrinder.getAllActivities()
            for idx, row in df_process.iterrows():
                object_id = row["objectId"]
                cultural_object = self.getEntityById(str(object_id))
                institute = row["responsibleInstitute"]
                person = row["responsiblePerson"]
                tool = set(row["tool"].split(', ')) if row["tool"] else set()
                start = row["startDate"]
                end = row["endDate"]
                internal_id = row["internalId"].lower()

                if "acquisition" in internal_id:
                    technique = row["technique"]
                    activity = impl.Acquisition(cultural_object, institute, person, tool, start, end, technique)
                elif "processing" in internal_id:
                    activity = impl.Processing(cultural_object, institute, person, tool, start, end)
                elif "exporting" in internal_id:
                    activity = impl.Exporting(cultural_object, institute, person, tool, start, end)
                elif "modelling" in internal_id:
                    activity = impl.Modelling(cultural_object, institute, person, tool, start, end)
                else:
                    activity = impl.Optimising(cultural_object, institute, person, tool, start, end)

                activities.append(activity)

        return activities
    
    def getActivitiesByResponsibleInstitution(self, partialName: str) -> list[impl.Activity]: # who is doing this
        pass

    def getActivitiesByResponsiblePerson(self, partialName: str) -> list[impl.Activity]:  # who
        pass

    def getActivitiesUsingTool(self, partial_name: str) -> list[impl.Activity]: # Lucrezia
        activities = []
        for activity in self.getAllActivities():
            tools = activity.getTools()
            if tools:
                for tool in tools:
                    if partial_name.lower() in tool.lower():
                        activities.append(activity)
                        break
        return activities
    
    def getActivitiesStartedAfter(self, date: str) -> list[impl.Activity]:  # who
        pass

    def getActivitiesEndedBefore(self, date: str) -> list[impl.Activity]:  # 
        pass

    def getAcquisitionsByTechnique(self, partialName: str) -> list[impl.Acquisition]:  #
        pass

class AdvancedMashup(BasicMashup):
    def getActivitiesOnObjectsAuthoredBy(self, personId: str) -> list[impl.Activity]:  #
        pass

    def getObjectsHandledByResponsiblePerson(self, partialName: str) -> list[impl.CulturalHeritageObject]:  #
        pass

    def getObjectsHandledByResponsibleInstitution(self, partialName: str) -> list[impl.CulturalHeritageObject]:  #
        pass

    def getAuthorsOfObjectsAcquiredInTimeFrame(self, start: str, end: str) -> list[impl.Person]:  # 
        pass
