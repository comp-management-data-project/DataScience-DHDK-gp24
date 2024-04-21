import json
from pyld import jsonld
import os

# Get the directory of the current script
current_directory = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(current_directory, 'process.json')

# Read JSON data from file
with open(file_path, 'r') as json_file:
    json_data = json.load(json_file)

# JSON-LD context to map keys to RDF terms
context = {
    "@context": {
        "@vocab": "http://w3id.org/data-science-dhdk",
        "object id": "@id",
        "acquisition": "hasAcquisition",
        "processing": "hasProcessing",
        "modelling": "hasModelling",
        "optimising": "hasOptimising",
        "exporting": "hasExporting",
        "responsible institute": "hasResponsibleInstitute",
        "responsible person": "hasResponsiblePerson",
        "technique": "hasTechnique",
        "tool": "hasTool",
        "start date": "hasStartDate",
        "end date": "hasEndDate"
    }
}

# Compact the JSON-LD using the context
compacted = jsonld.compact(json_data, context)

# Convert to pretty-printed JSON
json_ld = json.dumps(compacted, indent=2)

# Write the JSON-LD data to a new file
processLD_path = os.path.join(current_directory, 'processLD.jsonld')
with open(processLD_path, 'w') as processLD:
    processLD.write(json_ld)

print(f"JSON-LD data written to: {processLD_path}")
