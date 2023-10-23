import rdflib
from rdflib import URIRef

from enum import Enum
class PropertyType(Enum):
    LITERAL = 'literal'
    OBJECT = 'object'

class OntologyService:
    def __init__(self, ontologyContents, ontologyString=False):
        self.__ontology = rdflib.Graph()
        if ontologyString:
            self.__ontology.parse(data=ontologyContents, format='xml')
        else:
            self.__ontology.parse(ontologyContents)

    def predicateExists(self, predicateUri):
        return (URIRef(predicateUri), None, None) in self.__ontology
    
    def getPredicatePropertyType(self, predicateUri):
        result = self.__ontology.query("""
            PREFIX ldcm: <https://johanvansoest.nl/ontologies/LinkedDicom/>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX owl: <http://www.w3.org/2002/07/owl#>

            SELECT ?class
            WHERE {
                <%s> rdf:type ?class.
                FILTER (?class IN (owl:DatatypeProperty, owl:ObjectProperty))
            }
        """ % predicateUri)
        for row in result:
            if str(row["class"])=="http://www.w3.org/2002/07/owl#DatatypeProperty":
                return PropertyType.LITERAL
            if str(row["class"])=="http://www.w3.org/2002/07/owl#ObjectProperty":
                return PropertyType.OBJECT
    
    def relatedToInformationEntity(self, predicateUri):
        result = self.__ontology.query("""
            PREFIX ldcm: <https://johanvansoest.nl/ontologies/LinkedDicom/>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX owl: <http://www.w3.org/2002/07/owl#>

            SELECT ?class
            WHERE {
                <%s> ldcm:related_to_information_entity ?class.
            }
        """ % predicateUri)
        for row in result:
            return row["class"]

    def getRelatedSequenceItemClass(self, sequenceClass):
        result = self.__ontology.query("""
            PREFIX ldcm: <https://johanvansoest.nl/ontologies/LinkedDicom/>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX owl: <http://www.w3.org/2002/07/owl#>

            SELECT ?class
            WHERE {
                ?class ldcm:related_to_sequence <%s>;
                       rdfs:subClassOf* ldcm:Sequence_Item.
            }
        """ % sequenceClass)
        for row in result:
            return row["class"]
    
    def relatedToSequence(self, predicateUri):
        result = self.__ontology.query("""
            PREFIX ldcm: <https://johanvansoest.nl/ontologies/LinkedDicom/>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX owl: <http://www.w3.org/2002/07/owl#>

            SELECT ?class
            WHERE {
                <%s> ldcm:related_to_sequence ?class.
            }
        """ % predicateUri)
        for row in result:
            return row["class"]

    def getObjectPredicatesForClassRange(self, classUri):
        result = self.__ontology.query("""
            PREFIX ldcm: <https://johanvansoest.nl/ontologies/LinkedDicom/>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX owl: <http://www.w3.org/2002/07/owl#>

            SELECT ?predicate ?domain
            WHERE {
                <%s> rdfs:subClassOf* ?superClass.
                
                {
                    ?predicate rdfs:range <%s>.
                    ?predicate rdfs:domain ?domain.
                } UNION {
                    ?predicate rdfs:range ?superClass.
                    ?predicate rdfs:domain ?domain.
                }
            }
        """ % (classUri, classUri))
        return result
    
    def getKeyForClass(self, classUri):
        result = self.__ontology.query("""
            PREFIX ldcm: <https://johanvansoest.nl/ontologies/LinkedDicom/>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX owl: <http://www.w3.org/2002/07/owl#>

            SELECT ?tag
            WHERE {
                <%s> rdfs:subClassOf* ?superClass.

                { ?superClass ldcm:has_unique_identifier ?tag }
                UNION
                { <%s> ldcm:has_unique_identifier ?tag. }
            }
        """ % (classUri, classUri))
        for row in result:
            return row["tag"]
    
    def getClassForUID(self, uid):
        result = self.__ontology.query("""
            PREFIX ldcm: <https://johanvansoest.nl/ontologies/LinkedDicom/>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX owl: <http://www.w3.org/2002/07/owl#>

            SELECT ?class
            WHERE {
                ?class ldcm:has_sop_class_uid "%s".
            }
        """ % uid)
        for row in result:
            return row["class"]
        return "https://johanvansoest.nl/ontologies/LinkedDicom/Information_Object_Definition"
