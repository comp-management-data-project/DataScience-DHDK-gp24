from typing import Optional


# CH Objects Metadata classes

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
    def __init__(self, id: str, title: str, date: str, hasAuthor: list[Person], owner: str, place: str):
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

    def getAuthors(self) -> list[Person]: #Returns a list of authors (Person objects)
        return self.hasAuthor
    
    def getOwner(self) -> str:
        return self.owner
    
    def getPlace(self) -> str:
        return self.place


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




# Processes Data classes

class Activity(object): # Lucrezia
    def __init__(self, refersTo_cho: CulturalHeritageObject, institute: str, person: str, start: str, end: str, tool: set = set()):
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
    def __init__(self, refersTo_cho: CulturalHeritageObject, institute: str, person: str, start: str, end: str, technique: str, tool: set = set()):
        super().__init__(refersTo_cho, institute, person, start, end, tool)
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


# Basic Handlers classes
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
    
    idDataFrame = None

    def set_id_dataframe(self, dataframe):
        self.idDataFrame = dataframe

    def getById(self, id: str):
        if self.idDataFrame is None:
            raise ValueError("idDataFrame is not set. Call set_id_dataframe first.")
        id_value = self.idDataFrame["id"]
        return id_value