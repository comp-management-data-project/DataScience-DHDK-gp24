import impl

import os
from typing import Optional

# imports for relational db
from sqlite3 import connect
import json
from json import load

import sqlite3

# imports for graph db
from rdflib import RDF, Graph, URIRef, Literal
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
import SPARQLWrapper

# imports for both
import pandas as pd
from pandas import DataFrame, concat, read_csv, read_sql, Series




# Class to upload data from JSONs to SQLite database 
#Lucrezia

class ProcessDataUploadHandler(impl.Handler):
    def __init__(self, db_name="relational_database.db"):
        super().__init__()
        self.db_name = db_name

    def process_data(self, json_data):
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
        activity_dfs = {}
        tools_data = []
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
        except Exception as e:
            print(f"Error occurred while pushing data to DB: {str(e)}")

    def handle_duplicates(self, df, conn, table_name):
        existing_data = pd.read_sql(f"SELECT * FROM {table_name}", conn)
        df = df[~df.duplicated(keep='first')]
        if not existing_data.empty:
            df = pd.concat([existing_data, df]).drop_duplicates(keep='first')
        return df


# Load JSON data
file_path = os.path.join("resources", "process.json")
with open(file_path, "r") as file:
    json_data = json.load(file)

# Instantiate the ProcessDataUploadHandler
process_data_upload_handler = ProcessDataUploadHandler()

# Process the data
activity_dfs, tools_df = process_data_upload_handler.process_data(json_data)

# Push the data to the database
process_data_upload_handler.pushDataToDb(activity_dfs, tools_df)




# Class to upload CSV files to Blazegraph
class MetadataUploadHandler(impl.UploadHandler):
    def __init__(self, dbPathOrUrl):
        self.dbPathOrUrl = "";

    def pushDataToDb(self, path):
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
                full_author = row["Author"].strip('\"');
                author_id = full_author[full_author.find(":")+len(":"):full_author.rfind(")")];
                author_name = full_author.split("(")[0].strip();
                author_res_id = base_url + "Person/" + author_id;
                subj_author = URIRef(author_res_id);
                graph.add((subj, author, subj_author));
                graph.add((subj_author, title, Literal(str(author_name))));
                graph.add((subj_author, id, Literal(str(author_id))));
            # assigning owner
            if row["Owner"] != "":
                graph.add((subj, owner, Literal(row["Owner"])));
            # assigning place
            if row["Place"] != "":
                graph.add((subj, place, Literal(row["Place"])));

        store = SPARQLUpdateStore()

        # It opens the connection with the SPARQL endpoint instance
        store.open(("http://127.0.0.1:9999/blazegraph/sparql", "http://127.0.0.1:9999/blazegraph/sparql"))

        for triple in graph.triples((None, None, None)):
            store.add(triple)
            
        # Once finished, remeber to close the connection
        store.close()
        return True;
#option 1
class Handler:
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

class MetadataUploadHandler(UploadHandler):
    def __init__(self, dbPathOrUrl):
        super().__init__()
        self.dbPathOrUrl = dbPathOrUrl


    def pushDataToDb(self, path):
        # defining our base url
        base_url = "https://comp-data.github.io/res/"
        graph = Graph()
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
                full_author = row["Author"].strip('\"');
                author_id = full_author[full_author.find(":")+len(":"):full_author.rfind(")")];
                author_name = full_author.split("(")[0].strip();
                author_res_id = base_url + "Person/" + author_id;
                subj_author = URIRef(author_res_id);
                graph.add((subj, author, subj_author));
                graph.add((subj_author, title, Literal(str(author_name))));
                graph.add((subj_author, id, Literal(str(author_id))));
            # assigning owner
            if row["Owner"] != "":
                graph.add((subj, owner, Literal(row["Owner"])));
            # assigning place
            if row["Place"] != "":
                graph.add((subj, place, Literal(row["Place"])));
            ttl_content = graph.serialize(format="turtle")


        output_file = "output.ttl"
        with open(output_file, "w") as f:
            f.write(ttl_content)
# Create an instance of MetadataUploadHandler
metadata_handler = MetadataUploadHandler(dbPathOrUrl="output.ttl")

# Call the pushDataToDb() method with the path to the CSV file
metadata_handler.pushDataToDb("meta.csv")

#option 2
import re 
#could be a bad idea, not sure if it's even useful
def no_specials(text): #to remove special characters 
    remove = r'[^\w\s]'  
    return re.sub(remove, '', text)

    def pushDataToDb(self, path):
        # defining our base url
        base_url = "https://comp-data.github.io/res/";
        graph = Graph();
        # reading the data from the csv file and storing them into a dataframe
        df = pd.read_csv(path, 
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
        item = Namespace("https://github.com/ciao_a_tutti/")
        base_url = "https://github.com/ciao_a_tutti/"
        g = Graph()
        g.bind('item', item)

        #predicates
        Id = URIRef(item.Id)
        Type = URIRef(item.Type)
        Title = URIRef(item.Title)
        Date = URIRef(item.Date)
        Author = URIRef(item.Author)
        Owner = URIRef(item.Owner)
        Place = URIRef(item.Place)
        Viaf = URIRef(item.Viaf)

        #triples
        for index, row in df.iterrows():    

            subj = URIRef(item[str(index)])  
            if row["Id"] !='':
                g.add((subj, Id, Literal(row["Id"])))

            if row["Type"] !='':
                g.add((subj, Type, Literal(no_specials(row["Type"]))))
            if row["Title"] !='':
                g.add((subj, Title, Literal(no_specials(row["Title"]))))
            if row["Date"] !='':
                g.add((subj, Date, Literal(row["Date"]))) 

            if row["Owner"] !='':
                g.add((subj, Owner, Literal(no_specials(row["Owner"]))))

            if row["Owner"] !='':
                g.add((subj, Place, Literal(no_specials(row["Place"]))))
            
            authors = []

            if row["Author"] != '':
             if ';' in row["Author"]: #find out if there's multiple authors
                author_info = row["Author"].split(";")
                authors.extend([(author.strip()) for author in author_info]) #list with multiple authors
             else: 
                authors.append((row["Author"]))
             for author_info in authors:
              author_parts = author_info.split(" (") #split to find viaf
              author_only = author_parts[0].strip() 
             if len(author_parts) > 1:
                viaf_only = author_parts[1].split(")")[0].strip() #remove )
             else:
                viaf_only = ""  # viaf_only is an empty string if it's not present
            g.add((subj, Author, Literal(no_specials(author_only))))
            g.add((subj, Viaf, Literal(viaf_only)))
        
        # Bg       
        store = SPARQLUpdateStore()
        endpoint = 'http://10.201.11.240:9999/blazegraph/sparql'
        store.open((endpoint, endpoint))

        for triple in g.triples((None, None, None)):
            store.add(triple)
        
        store.close()
