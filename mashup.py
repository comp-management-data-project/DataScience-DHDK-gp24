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
import impl, query, upload

class BasicMashup:  #Hubert
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

    # helper method to reduce code clutter # Hubert
    # column names here might have to be changed, depending on your implementation of loading data to sql
    def createActivityList(self, df): 
        activities = [];
        for idx, row in df.iterrows():
            if row["Activity_internal_id"].contains("acquisition"):
                activity = impl.Acquisition(impl.CulturalHeritageObject(row["activity_internal_id"].rsplit('-'), "", "", "", "", ""), row["Responsible Institute"], row["Responsible Person"], row["Tool"], row["Start Date"], row["End Date"], row["Technique"]);
                activities.append(activity);
            if row["Activity_internal_id"].contains("processing"):
                activity = impl.Processing(impl.CulturalHeritageObject(row["activity_internal_id"].rsplit('-'), "", "", "", "", ""), row["Responsible Institute"], row["Responsible Person"], row["Tool"], row["Start Date"], row["End Date"]);
                activities.append(activity);
            if row["Activity_internal_id"].contains("modelling"):
                activity = impl.Modelling(impl.CulturalHeritageObject(row["activity_internal_id"].rsplit('-'), "", "", "", "", ""), row["Responsible Institute"], row["Responsible Person"], row["Tool"], row["Start Date"], row["End Date"]);
                activities.append(activity);
            if row["Activity_internal_id"].contains("optimising"):
                activity = impl.Optimising(impl.CulturalHeritageObject(row["activity_internal_id"].rsplit('-'), "", "", "", "", ""), row["Responsible Institute"], row["Responsible Person"], row["Tool"], row["Start Date"], row["End Date"]);
                activities.append(activity);
            if row["Activity_internal_id"].contains("exporting"):
                activity = impl.Exporting(impl.CulturalHeritageObject(row["activity_internal_id"].rsplit('-'), "", "", "", "", ""), row["Responsible Institute"], row["Responsible Person"], row["Tool"], row["Start Date"], row["End Date"]);
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

    def getAllActivities(self) -> list[impl.Activity]:  # Lucrezia
        activities = dict()
        for processGrinder in self.processQuery:
            df_process = processGrinder.getAllActivities()
            for idx, row in df_process.iterrows():
                object_id = row["objectId"]
                cultural_object = self.getEntityById(str(object_id))
                institute = row["responsibleInstitute"]
                person = row["responsiblePerson"] if row["responsiblePerson"] else None
                tool = set(row["tool"].split(', ')) if row["tool"] else set()
                start = row["startDate"] if row["startDate"] else None
                end = row["endDate"] if row["endDate"] else None
                internal = row["internalId"]

                if "acquisition" in internal.lower():
                    technique = row["technique"]
                    activity = impl.Acquisition(technique, institute, cultural_object, person, tool, start, end)
                elif "processing" in internal:
                    activity = impl.Processing(institute, cultural_object, person, tool, start, end)
                elif "exporting" in internal:
                    activity = impl.Exporting(institute, cultural_object, person, tool, start, end)
                elif "modelling" in internal:
                    activity = impl.Modelling(institute, cultural_object, person, tool, start, end)
                else:
                    activity = impl.Optimising(institute, cultural_object, person, tool, start, end)

                activities[internal] = activity

        return list(activities.values())
    
    def getActivitiesByResponsibleInstitution(self, partialName: str) -> list[impl.Activity]: # who is doing this
        pass

    def getActivitiesByResponsiblePerson(self, partialName: str) -> list[impl.Activity]:  # who
        pass

    def getActivitiesUsingTool(self, partialName: str) -> list[impl.Activity]:  # Lucrezia
        activities = []
        for activity in self.getAllActivities():
            tools = activity.getTools()
            if tools:
                for tool in tools:
                    if partialName.lower() in tool.lower():
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
