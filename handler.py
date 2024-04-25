# imports for relational db
from sqlite3 import connect
from json import load

# imports for graph db
from rdflib import RDF, Graph, URIRef, Literal
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
import SPARQLWrapper

# imports for both
import pandas as pd
from pandas import DataFrame, concat, read_csv, read_sql, Series

from pprint import pprint as pp


# handlers # Hubert

class Handler:
    def __init__(self):
        self.dbPathOrUrl = ""

    def __init__(self, dbPathOrUrl): # overrides the first init but this class itself is never created so oh well
        self.dbPathOrUrl = dbPathOrUrl

    def getDbPathOrUrl(self):
        return self.dbPathOrUrl

    def setDbPathOrUrl(self, pathOrUrl):
        self.dbPathOrUrl = pathOrUrl
        if self.dbPathOrUrl == pathOrUrl:
            return True
        return False


class UploadHandler(Handler):
    def __init__(self):
        self.dbPathOrUrl = ""

    def __init__(self, dbPathOrUrl): # overrides the first init but this class itself is never created so oh well
        self.dbPathOrUrl = dbPathOrUrl

    def pushDataToDb(path): #shouldn't be specified 'self' as a parameter?
        return False



# Class to upload data from JSONs to SQLite database (incomplete) #Lucrezia

class ProcessDataUploadHandler(UploadHandler):
    def __init__(self):
        super().__init__()

    def pushDataToDb(self, json_file: str) -> bool:
        with open(json_file) as f:
            list_of_dict = load(f)
            keys = list_of_dict[0].keys()
            j_df = DataFrame(list_of_dict, columns=keys)
            j_df = j_df.drop_duplicates(subset='object id', keep='first')#removing all duplicates from the dataframe



class QueryHandler(Handler): 
    def __init__(self):
        self.dbPathOrUrl = ""

    def __init__(self, dbPathOrUrl): # overrides the first init but this class itself is never created so oh well
        self.dbPathOrUrl = dbPathOrUrl

    def getById(self, id):
        return None; # never accessed here and overridden in child class



# Class to interact with SQL database running queries

class ProcessDataQueryHandler(QueryHandler): #Lucrezia
    def __init__(self):
        self.dbPathOrUrl = ""

        # the 7 dataframes (queries) for ProcessDataQueryHandler need to be implemented

    def getAllActivities(self) -> DataFrame:
        with connect(self.getDbPathOrUrl()) as con:
            query_all_activities = f"""
            """

            df_all_activities = read_sql(query_all_activities, con)

        return df_all_activities
    
    def getActivitiesByResponsibleInstitution(self, partialName: str) -> DataFrame:
        with connect(self.getDbPathOrUrl()) as con:
            query = f"""
            """
            df = read_sql(query, con)
        return df
    
    def getActivitiesByResponsiblePerson(self, partialName: str) -> DataFrame:
        with connect(self.getDbPathOrUrl()) as con:
            query = f"""
            """

            df = read_sql(query, con)
        return df
    
    def getActivitiesUsingTool(self, partialName: str) -> DataFrame:
        with connect(self.getDbPathOrUrl()) as con:
            query = f"""
            """

            df = read_sql(query, con)
        return df
    
    def getActivitiesStartedAfter(self, date: str) -> DataFrame:
        with connect(self.getDbPathOrUrl()) as con:
            query = f"""
            """

            df = read_sql(query, con)
        return df
    
    def getActivitiesEndedBefore(self, date: str) -> DataFrame:
        with connect(self.getDbPathOrUrl()) as con:
            query = f"""
            """

            df = read_sql(query, con)
        return df

    def getAcquisitionsByTechnique(self, partialName: str) -> DataFrame:
        with connect(self.getDbPathOrUrl()) as con:
            query = f"""
            """

            df = read_sql(query, con)
        return df    
        
        
        


class MetadataQueryHandler(QueryHandler):
    def __init__(self):
        self.dbPathOrUrl = ""

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

    
    # helper method to reduce code clutter
    def executeQuery(self, sql_command): 
        connection = connect(self.dbPathOrUrl);
        cursor = connection.cursor();
        cursor.execute(sql_command);
        df = pd.DataFrame(cursor.fetchall(), columns = [description[0] for description in cursor.description]);
        connection.close();
        return df;

    # UML methods go here