from typing import Optional


# CH Objects Data classes

class IdentifiableEntity(object):
    def __init__(self, id: str):
        self.id = id

    def getId(self):
        return self.id


class Person(IdentifiableEntity):
    def __init__(self, id: str, name: str):
        self.name = name
        super().__init__(id)

    def getName(self):
        return self.name


class CulturalHeritageObject(IdentifiableEntity):
    def __init__(self, id: str, title: str, owner: str, place: str, date: str = None, hasAuthor: list = None):
        self.title = title
        self.date = date
        self.owner = owner
        self.place = place
        self.hasAuthor = hasAuthor or []

        super().__init__(id)

    def getTitle(self) -> str:
        return self.title

    def getDate(self) -> Optional[str]:
        return self.date

    def getOwner(self) -> str:
        return self.owner

    def getPlace(self) -> str:
        return self.place

    def getAuthors(self) -> list: #Returns a list of authors (Person objects)
        return self.hasAuthor


class NauticalChart(CulturalHeritageObject):
    pass


class ManuscriptPlate(CulturalHeritageObject):
    pass


class ManuscriptVolume(CulturalHeritageObject):
    pass


class PrintedVolume(CulturalHeritageObject):
    pass


class PrintedMaterial(CulturalHeritageObject):
    pass


class Herbarium(CulturalHeritageObject):
    pass


class Specimen(CulturalHeritageObject):
    pass


class Painting(CulturalHeritageObject):
    pass


class Model(CulturalHeritageObject):
    pass


class Map(CulturalHeritageObject):
    pass




# Processes Data class

class Activity(object):
    def __init__(self, refersTo_cho: CulturalHeritageObject, institute: str, person: str = None, tool: set = set(), start: str = None, end: str = None):
        self.refersTo_cho = refersTo_cho
        self.institute = institute
        self.person = person
        self.tool = tool
        self.start = start
        self.end = end

    def getResponsibleInstitute(self) -> str:
        return self.institute
    
    def getResponsiblePerson(self) -> Optional[str]:
        return self.person
    
    def getTools(self) -> set: # getTool has arity zero or more [0..*]
        return self.tool
    
    def getStartDate(self) -> Optional[str]:
        return self.start

    def getEndDate(self) -> Optional[str]:
        return self.end
    
    def getRefersTo_cho(self) -> CulturalHeritageObject:
        return self.refersTo_cho


class Acquisition(Activity):
    def __init__(self, refersTo_cho: CulturalHeritageObject, institute: str, person: str, tool: set, start: str, end: str, technique: str):
        super().__init__(refersTo_cho, institute, person, tool, start, end)
        self.technique = technique
    
    def getTechnique(self) -> str:
        return self.technique

class Processing(Activity):
    pass

class Modelling(Activity):
    pass

class Optimising(Activity):
    pass

class Exporting(Activity):
    pass


# Basic handlers 
# Hubert

class Handler(object):
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

    def pushDataToDb(self):
        pass


class QueryHandler(Handler): 
    def __init__(self, dbPathOrUrl=""):  # Provide a default value for dbPathOrUrl
        super().__init__()
        self.dbPathOrUrl = dbPathOrUrl
    
    def getById(self, id):
        idDataFrame = idDataFrame["id"]
        self.getById = idDataFrame
        return self.getById
    
    def MetadataQueryHandler(QueryHandler):
        def getAllPeople(self):
            return

    def getById(self, id):
        return None
