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
        self.tool = set(tool) 
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