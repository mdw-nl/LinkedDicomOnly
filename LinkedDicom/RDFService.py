import rdflib
from rdflib import RDF, RDFS, Literal, URIRef
import urllib.parse

class GraphService:
    def __init__(self, filePath=None):
        self.__graph = rdflib.Graph()
        self.__graph.bind('ldcm', 'https://johanvansoest.nl/ontologies/LinkedDicom/')
        self.__graph.bind('data', 'http://data.local/rdf/linkeddicom/')
        self.__graph.bind('rdfs', 'http://www.w3.org/2000/01/rdf-schema#')
        self.__graph.bind('schema', 'https://schema.org/')
        self.__graph.bind('file', 'file:/')

        if filePath is not None:
            self.__graph.parse(filePath, format=rdflib.util.guess_format(filePath))

    def replaceUriToShort(self, uriString):
        for ns in self.__graph.namespaces():
            uriString = uriString.replace(str(ns[1]), str(ns[0]) + ":")
        return uriString
    
    def replaceShortToUri(self, iriString):
        content = iriString[iriString.find(":")+1:]
        content = urllib.parse.quote(content)
        prefix = iriString[0:iriString.find(":")+1]
        iriString = f"{prefix}{content}"
        
        for ns in self.__graph.namespaces():
            iriString = iriString.replace(str(ns[0]) + ":", str(ns[1]))
        return URIRef(iriString)
    
    def removeNamespaceFromClass(self, iriString):
        for ns in self.__graph.namespaces():
            iriString = iriString.replace(str(ns[0]) + ":", "")
        return iriString

    def valueAsIri(self, value):
        value = urllib.parse.quote(value)
        return self.replaceShortToUri("data:" + value)

    def instanceIriExists(self, iriString):
        return (self.replaceShortToUri(iriString), None, None) in self.__graph

    def createOrGetInstance(self, classUri, identifier, identifierPredicate=None):
        iriClass = self.replaceUriToShort(classUri)
        if identifier.startswith("data:"):
            instanceIri = identifier
        else:
            instanceIri = "data:%s" % identifier

        if not self.instanceIriExists(instanceIri) :
            self.__graph.add([self.replaceShortToUri(instanceIri), RDF.type, self.replaceShortToUri(iriClass)])
            if identifierPredicate is not None:
                self.__graph.add([self.replaceShortToUri(instanceIri), self.replaceShortToUri(identifierPredicate), Literal(identifier)])
        
        return instanceIri

    def addPredicateLiteralToInstance(self, instanceIri, predicate, value):
        if not self.instanceIriExists(instanceIri):
            raise Exception("Instance IRI does not exist")
        
        self.__graph.add([self.replaceShortToUri(instanceIri), self.replaceShortToUri(predicate), Literal(value)])
    
    def addPredicateObjectToInstance(self, instanceIri, predicate, value):
        if not self.instanceIriExists(instanceIri):
            raise Exception("Instance IRI does not exist")
        
        self.__graph.add([self.replaceShortToUri(instanceIri), self.replaceShortToUri(predicate), self.replaceShortToUri(value)])
    
    def getAllTriples(self):
        # return str(self.__graph.serialize(format="n3"), 'utf-8')
        allTriplesSerialized = self.__graph.serialize(format="n3")
        if not (type(allTriplesSerialized) == str):
            allTriplesSerialized = allTriplesSerialized.decode('utf-8')
        return allTriplesSerialized
    
    def saveTriples(self, filePath):
        with open(filePath, "w") as text_file:
            text_file.write(self.getAllTriples())
    
    def getTriplesTurtle(self):
        return self.__graph.serialize(format='nt')
    
    def runSparqlQuery(self, queryString):
        return self.__graph.query(queryString)
