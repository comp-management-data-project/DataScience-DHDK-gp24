

# CH Objects Metadata classes


# Process Data classes
class Activity(object): # plus relation = Activity "refersTo" cho
    def __init__(self, refersTo: CulturalHeritageObject, institute, person: str = None, tool: set = set(), start: str = None, end: str = None):
        self.institute = institute 
        self.person = person
        self.tool = tool
        self.start = start
        self.end = end
        self.refersTo = refersTo

    def getResponsibleInstitute(self) -> str:
        return self.institute
    
    def getResponsiblePerson(self) -> str | None:
        return self.person
    
    def getTools(self) -> set: # getTool has arity zero or more [0..*]
        return self.tool
    
    def getStartDate(self) -> str | None:
        return self.start

    def getEndDate(self) -> str | None:
        return self.end
    
    def refersTo(self) -> CulturalHeritageObject:
        return self.culturalHeritageObject


class Acquisition(Activity):
    def __init__(self, institute, person, tool, start, end, technique: str):
        super().__init__(id, institute, person, tool, start, end)
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