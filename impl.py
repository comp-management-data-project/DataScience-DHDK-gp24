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

class Activity(object): # Activity "refersTo" CHO
    def __init__(self, refersTo: CulturalHeritageObject, institute: str, person: str = None, tool: set = set(), start: str = None, end: str = None):
        self.refersTo = refersTo
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
    
    def getRefersTo(self) -> CulturalHeritageObject:
        return self.refersTo


class Acquisition(Activity):
    def __init__(self, refersTo: CulturalHeritageObject, institute: str, person: str, tool: set, start: str, end: str, technique: str):
        super().__init__(refersTo, institute, person, list(tool), start, end) # Convert tool set to list
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