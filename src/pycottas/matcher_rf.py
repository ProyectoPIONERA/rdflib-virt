from rdflib import Graph, Namespace
import re

# Namespaces
RML = Namespace("http://w3id.org/rml/")
RR  = Namespace("http://www.w3.org/ns/r2rml#")

QUERY = """
SELECT DISTINCT ?tm ?predicateValue ?object ?template WHERE { 
    ?tm rml:predicateObjectMap ?pom .
    
    ?pom rml:predicateMap ?pm .
    ?pm rml:constant ?predicateValue .

    OPTIONAL {
        ?pom rml:objectMap ?om .
        ?om rml:constant ?object .
    }

    OPTIONAL {
        ?tm rml:subjectMap ?sm .
        ?sm rml:template ?template .
    }
}
"""

def extract_bound_pattern(mapping_file):
    """Extrae valores bounded del mapping generado TTL"""
    graph = Graph()
    graph.parse(mapping_file, format="turtle")

    rows = list(graph.query(QUERY, initNs={"rml": RML, "rr": RR}))

    subjects = set()
    predicates = set()
    objects = set()

    for row in rows:
        if row.template:
            subjects.add(str(row.template))
        if row.predicateValue:
            predicates.add(str(row.predicateValue))
        if row.object:
            objects.add(str(row.object))

    return {
        "subjects": list(subjects),
        "predicates": list(predicates),
        "objects": list(objects)
    }
