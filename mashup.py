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

#datetime manipulation
from datetime import datetime


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
    
    def cleanMetadataHandlers(self): # Hubert
        self.metadataQuery = [];
        return True;

    def cleanProcessHandlers(self): # Hubert
        self.processQuery = [];
        return True;

    def addMetadataHandler(self, handler: query.MetadataQueryHandler):
        self.metadataQuery.append(handler);
        return True;

    def addProcessHandler(self, handler: query.ProcessDataQueryHandler):
        self.processQuery.append(handler);
        return True;

    # helper method to reduce code clutter # Hubert
    def createActivityList(self, df): # Hubert 
        activities = []
        
        # formatted version Lucrezia
        for idx, row in df.iterrows():
            if row["type"] == "acquisition":
                activity = impl.Acquisition(
                    row["responsible_institute"],
                    row["responsible_person"],
                    row["tool"],
                    row["start_date"],
                    row["end_date"],
                    row["object_id"],
                    row["technique"]
                )
            elif "acquisition" in row["Activity_internal_id"]:
                activity = impl.Acquisition(
                    impl.CulturalHeritageObject(row["Activity_internal_id"].rsplit('-'), "", "", "", "", ""),
                    row["Responsible Institute"],
                    row["Responsible Person"],
                    row["Tool"],
                    row["Start Date"],
                    row["End Date"],
                    row["Technique"]
                )
                activities.append(activity)
            elif row["type"] == "processing":
                activity = impl.Processing(
                    row["responsible_institute"],
                    row["responsible_person"],
                    row["tool"],
                    row["start_date"],
                    row["end_date"],
                    row["object_id"]
                )
            elif "processing" in row["Activity_internal_id"]:
                activity = impl.Processing(
                    impl.CulturalHeritageObject(row["activity_internal_id"].rsplit('-'), "", "", "", "", ""),
                    row["Responsible Institute"],
                    row["Responsible Person"],
                    row["Tool"],
                    row["Start Date"],
                    row["End Date"]
                )
                activities.append(activity)
            elif row["type"] == "modelling":
                activity = impl.Modelling(
                    row["responsible_institute"],
                    row["responsible_person"],
                    row["tool"],
                    row["start_date"],
                    row["end_date"],
                    row["object_id"]
                )
            elif "modelling" in row["Activity_internal_id"]:
                activity = impl.Modelling(
                    impl.CulturalHeritageObject(row["activity_internal_id"].rsplit('-'), "", "", "", "", ""),
                    row["Responsible Institute"],
                    row["Responsible Person"],
                    row["Tool"],
                    row["Start Date"],
                    row["End Date"]
                )
                activities.append(activity)
            elif row["type"] == "optimising":
                activity = impl.Optimising(
                    row["responsible_institute"],
                    row["responsible_person"],
                    row["tool"],
                    row["start_date"],
                    row["end_date"],
                    row["object_id"]
                )
            elif "optimising" in row["Activity_internal_id"]:
                activity = impl.Optimising(
                    impl.CulturalHeritageObject(row["activity_internal_id"].rsplit('-'), "", "", "", "", ""),
                    row["Responsible Institute"],
                    row["Responsible Person"],
                    row["Tool"],
                    row["Start Date"],
                    row["End Date"]
                )
                activities.append(activity)
            elif row["type"] == "exporting":
                activity = impl.Exporting(
                    row["responsible_institute"],
                    row["responsible_person"],
                    row["tool"],
                    row["start_date"],
                    row["end_date"],
                    row["object_id"]
                )
            elif "exporting" in row["Activity_internal_id"]:
                activity = impl.Exporting(
                    impl.CulturalHeritageObject(row["activity_internal_id"].rsplit('-'), "", "", "", "", ""),
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

    def getEntityById(self, cho_df, entity_id: str) -> impl.IdentifiableEntity | None:  #Giorgia
        entity_list = []
        for entity in self.createObjectList(cho_df):
            if entity.id == entity_id:
                entity_list.append((entity, obj))
        if entity_list:
            return entity_list  
        else: 
            return None

    def getAllCulturalHeritageObjects(self, cho_df) -> list[impl.CulturalHeritageObject]:  #Iheb
       cho_list =  self.createObjectList()
       return cho_list

    def getAuthorsOfCulturalHeritageObject(self, cho_df, objectId: str) -> list[impl.Person]:  # Iheb
        cho_list = self.createObjectList()
        author_list = [author for obj in cho_list if obj.id == objectId for author in obj.author]
        return author_list
    
    def getCulturalHeritageObjectsAuthoredBy(self, cho_df, AuthorId: str) -> list[impl.CulturalHeritageObject]:  #Iheb
        cho_list = self.createObjectList()
        cho_authoredBy = []
        for obj in cho_list:
            for author in obj.author:
                if AuthorId.lower() in author.name.lower():
                    cho_authoredBy.append((obj, obj.title))
                    break
        return cho_authoredBy

    def getAllActivities(self, df):  # Lucrezia
        return self.createActivityList(df)
    
    def getActivitiesByResponsibleInstitution(self, df, partialName: str) -> list[impl.Activity]:  # Giorgia
        activities = self.createActivityList(df)
        resp_inst_filtered_activities = [activity for activity in activities if partialName in activity.responsible_institution]
        return resp_inst_filtered_activities
    
    def getActivitiesByResponsiblePerson(self, df, partialName: str) -> list[impl.Activity]:  # Giorgia
        activities = self.createActivityList(df)
        resp_pers_filtered_activities = [activity for activity in activities if partialName in activity.responsible_person]
        return resp_pers_filtered_activities
        
    def getActivitiesUsingTool(self, df, partial_name: str):  # Lucrezia
        activities = self.createActivityList(df)
        filtered_activities = [activity for activity in activities if partial_name in impl.Activity.getTools()]
        return filtered_activities
    
    def getActivitiesStartedAfter(self, df, date: str) -> list[impl.Activity]:  # Giorgia
        start_date = datetime.strptime(date, "%Y-%m-%d")   # from date to a datetime obj (I am unsure about the "%Y-%m-%d")

        activities = self.createActivityList(df) #activities list from df
        after_date_filtered_activities = [activity for activity in activities if datetime.strptime(activity.start_date, "%Y-%m-%d") >= start_date] #grab only activities after specific date 
        
        return after_date_filtered_activities

    def getActivitiesEndedBefore(self, df, date: str) -> list[impl.Activity]: #Giorgia
        end_date = datetime.strptime(date, "%Y-%m-%d")   # from date to a datetime obj

        activities = self.createActivityList(df) #activities list from df
        before_date_filtered_activities = [activity for activity in activities if datetime.strptime(activity.end_date, "%Y-%m-%d") <= end_date] #grab only activities before specific date 
        
        return before_date_filtered_activities

    def getAcquisitionsByTechnique(self, df, technique: str) -> list[impl.Acquisition]: #Giorgia
        activities = self.createActivityList(df)
        matching_acquisitions = [activity for activity in activities if isinstance(activity, impl.Acquisition) and technique in activity.technique] #only matching activities and technique: is activity an instance of class impl.Acquisition and is technique a substring of activity.technique
        return matching_acquisitions

class AdvancedMashup(BasicMashup):
    def getActivitiesOnObjectsAuthoredBy(self, personId: str) -> list[impl.Activity]:  #
        pass

    def getObjectsHandledByResponsiblePerson(self, partialName: str) -> list[impl.CulturalHeritageObject]:  #
        pass

    def getObjectsHandledByResponsibleInstitution(self, partialName: str) -> list[impl.CulturalHeritageObject]:  #
        pass

    def getAuthorsOfObjectsAcquiredInTimeFrame(self, start: str, end: str) -> list[impl.Person]:  # 
        pass
