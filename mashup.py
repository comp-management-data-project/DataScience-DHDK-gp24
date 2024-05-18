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
    def getEntityById(self, cho_df, entity_id: str) -> impl.IdentifiableEntity | None:  #Giorgia
        entity_list = []
        for entity in self.createObjectList(cho_df):
            if entity.id == entity_id:
                entity_list.append((entity, obj))
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
                person = impl.Person(row["id"], row["name"]);
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
    def getAllCulturalHeritageObjects(self) -> list[impl.CulturalHeritageObject]:  #Iheb
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

    def getAuthorsOfCulturalHeritageObject(self, objectId: str) -> list[impl.Person]:
        author_list = []
        if len(self.metadataQuery) > 0:
            author_df = self.metadataQuery[0].getAuthorsOfCulturalHeritageObject(objectId)
            for idx, row in author_df.iterrows():
                person = impl.Person(row["id"], row["name"])
                author_list.append(person)
        return author_list



    """
    this is basically getAllCulturalHeritageObjects with one line changed:
    - call getCulturalHeritageObjectsAuthoredBy(AuthorId) instead of getAllCulturalHeritageObjects()
    """
    def getCulturalHeritageObjectsAuthoredBy(self, AuthorId: str) -> list[impl.CulturalHeritageObject]:  #Iheb
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
    def getAllActivities(self):
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
    def getActivitiesByResponsibleInstitution(self, partialName: str) -> list[impl.Activity]:
        activities = []  
        if len(self.processQuery) > 0:  # check handlers 
            activities_df = self.processQuery[0].getResponsibleInstitute(partialName)  
            activities = self.createActivityList(activities_df) 
        return activities  

    """
    basically the same as getAllActivities, just use a different method from ProcessDataQueryHandler
    use filtering in SQL
    """
    def getActivitiesByResponsiblePerson(self, df, partialName: str) -> list[impl.Activity]:  # Giorgia
        activities = self.createActivityList(df)
        resp_pers_filtered_activities = [activity for activity in activities if partialName in activity.responsible_person]
        return resp_pers_filtered_activities

    """
    basically the same as getAllActivities, just use a different method from ProcessDataQueryHandler
    use filtering in SQL
    """
    def getActivitiesUsingTool(self, df, partial_name: str):  # Lucrezia
        activities = self.createActivityList(df)
        filtered_activities = [activity for activity in activities if partial_name in impl.Activity.getTools()]
        return filtered_activities

    """
    basically the same as getAllActivities, just use a different method from ProcessDataQueryHandler
    use filtering in SQL
    """
    def getActivitiesStartedAfter(self, df, date: str) -> list[impl.Activity]:  # Giorgia
        start_date = datetime.strptime(date, "%Y-%m-%d")   # from date to a datetime obj (I am unsure about the "%Y-%m-%d")

        activities = self.createActivityList(df) #activities list from df
        after_date_filtered_activities = [activity for activity in activities if datetime.strptime(activity.start_date, "%Y-%m-%d") >= start_date] #grab only activities after specific date 
        
        return after_date_filtered_activities

    """
    basically the same as getAllActivities, just use a different method from ProcessDataQueryHandler
    use filtering in SQL
    """
    def getActivitiesEndedBefore(self, df, date: str) -> list[impl.Activity]: #Giorgia
        end_date = datetime.strptime(date, "%Y-%m-%d")   # from date to a datetime obj

        activities = self.createActivityList(df) #activities list from df
        before_date_filtered_activities = [activity for activity in activities if datetime.strptime(activity.end_date, "%Y-%m-%d") <= end_date] #grab only activities before specific date 
        
        return before_date_filtered_activities

    """
    basically the same as getAllActivities, just use a different method from ProcessDataQueryHandler
    use filtering in SQL
    """
    def getAcquisitionsByTechnique(self, df, technique: str) -> list[impl.Acquisition]: #Giorgia
        activities = self.createActivityList(df)
        matching_acquisitions = [activity for activity in activities if isinstance(activity, impl.Acquisition) and technique in activity.technique] #only matching activities and technique: is activity an instance of class impl.Acquisition and is technique a substring of activity.technique
        return matching_acquisitions

class AdvancedMashup(BasicMashup):
    #I've written two things; I don't really know if either of them are correct to be honest 
    def getActivitiesOnObjectsAuthoredBy(self, personId: str) -> list[impl.Activity]: #fairly sure it's wrong
        activities = []
        if len(self.processQuery) > 0:  # Check for handlers
            activities_df = self.processQuery[0].getActivitiesOnObjectsAuthoredBy(personId)  # Activities with the same author

            for idx, row in activities_df.iterrows():
                # Determine the class based on the row information (e.g., type of activity)
                if row['ActivityType'] == 'Acquisition':
                    activity = impl.Acquisition(
                        refers_to_cho=self.getEntityById(row['id']),  
                        institute=row['Responsible Institute'],
                        person=row['Responsible Person'],
                        start=row['Start Date'],
                        end=row['End Date'],
                        technique=row['Technique'],
                        
                    )
                elif: 
                # all the other types in activities
                else:
                    activity = impl.Activity(
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

'''option 2
        def getActivitiesOnObjectsAuthoredBy(self, personId: str) -> list[impl.Activity]:
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
                
                return activities_on_objects'''

    def getObjectsHandledByResponsiblePerson(self, partialName: str) -> list[impl.CulturalHeritageObject]:  #
        pass

    def getObjectsHandledByResponsibleInstitution(self, partialName: str) -> list[impl.CulturalHeritageObject]:  #
        Objects = []
        if len(self.processQuery) > 0:
            institutions_df = self.processQuery[0].getActivitiesByResponsibleInstitution(partialName)
            activities = self.createActivityList(institutions_df)
            if len(self.metadataQuery) > 0:
                objects_df = self.metadataQuery[0].getAllCulturalHeritageObjects()
                object_list = self.createObjectList(objects_df)
                object_ids = []
                for activity in activities:
                    activity_id =activity.refersTo_cho.id
                    if activity_id not in object_ids:
                        object_ids.append(activity_id)
                        Objects.append(object)
        return Objects

    def getAuthorsOfObjectsAcquiredInTimeFrame(self, start: str, end: str) -> list[impl.Person]:  # 
        Objects = []
        if len(self.processQuery) > 0:
            institutions_df_end = self.getActivitiesEndedBefore(end)
            institutions_df_start = self.getActivitiesStartedAfter(start)
            activities_df = pd.merge(institutions_df_start, institutions_df_end, how="inner", on=["Activity_internal_id"])
            activity_list = self.createActivityList(activities_df)
            
            for activity in activity_list:
                if activity.refersTo_cho.person[0]  and activity.refersTo_cho.person[0] not in Objects: 
                    author = activity.refersTo_cho.person[0]    
                    Objects.append(author)
        return Objects