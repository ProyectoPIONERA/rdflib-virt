import pandas as pd
import rdflib
from rdflib import Graph, Namespace, URIRef, Literal
import pandas as pd
import morph_kgc 
from morph_kgc.config import Config
from morph_kgc.mapping.mapping_parser import retrieve_mappings
#from matcher_rf import extract_bound_pattern
from rdflib import Graph
import re
import tempfile


config = Config()
config.read("config.ini")
config.complete_configuration_with_defaults()
config.validate_configuration_section()
rml_df, _, _ = retrieve_mappings(config)
rml_df.to_csv("output_rml.csv", index=False)

print("dataframe generated: output_rml.csv (step 1)")

def rml_df_to_ttl(csv_path, ttl_path):

    df = pd.read_csv(csv_path)

    RML = Namespace("http://w3id.org/rml/")
    RR = Namespace("http://www.w3.org/ns/r2rml#")
    RDF = Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#")
    UB = Namespace("http://swat.cse.lehigh.edu/onto/univ-bench.owl#")

    g = Graph()
    g.bind("rml", RML)
    g.bind("rr", RR)
    g.bind("rdf", RDF)
    g.bind("ub", UB)

    for _, row in df.iterrows():

        tm_uri = URIRef(row["triples_map_id"])
        g.add((tm_uri, RDF.type, RML.TriplesMap))

        

        # Logical Source
        ls_bnode = rdflib.BNode()
        ls_type = (row["logical_source_type"]).split("/")[-1].lower()
        ls_value = (row["logical_source_value"])
        

        g.add((tm_uri, RML.logicalSource, ls_bnode))
        g.add((ls_bnode, RML[ls_type], Literal(ls_value)))

        if ls_type == "query":
            g.add((ls_bnode, RML.referenceFormulation, RML.SQL2008))

        # Subject Map ==================
        subj_bnode = rdflib.BNode()
        subj_type = (row["subject_map_type"]).split("/")[-1].lower()
        subj_value = row["subject_map_value"]
        subj_termtype = row["subject_termtype"].split("/")[-1]

        g.add((tm_uri, RML.subjectMap, subj_bnode))
        g.add((subj_bnode, RML[subj_type], Literal(subj_value)))
        g.add((subj_bnode, RML.termType, RML[subj_termtype]))

        # PredicateObjectMap ===========================
        pom_bnode = rdflib.BNode()
        g.add((tm_uri, RML.predicateObjectMap, pom_bnode))

        # Predicate Map ============================
        pred_bnode = rdflib.BNode()
        pred_type = (row["predicate_map_type"]).split("/")[-1].lower()
        pred_val = (row["predicate_map_value"]).split("#")[-1].lower()

        g.add((pom_bnode, RML.predicateMap, pred_bnode))
        g.add((pred_bnode, RML[pred_type], UB[pred_val]))

        g.add((pred_bnode, RML.termType, RML.IRI))

        # Object Map ======================================
        obj_bnode = rdflib.BNode()
        obj_type = (row["object_map_type"]).split("/")[-1].lower()
        obj_value = row["object_map_value"]
        obj_value_short = (row["object_map_value"]).split("#")[-1].lower()
        obj_termtype = (row["object_termtype"]).split("/")[-1]

        g.add((pom_bnode, RML.objectMap, obj_bnode))


        if "constant" in obj_type:
            g.add((obj_bnode, RML[obj_type], UB[obj_value_short]))
        elif "reference" or "template" in obj_type:
            g.add((obj_bnode, RML[obj_type], Literal(obj_value)))


        g.add((obj_bnode, RML.termType, RML[obj_termtype]))


        # Graph Map ======================================
        if isinstance(row["graph_map_type"], str) and row["graph_map_type"].strip():
            gm_bnode = rdflib.BNode()
            gm_type = row["graph_map_type"].split("/")[-1].lower()
            gm_value = row["graph_map_value"].split("/")[-1]
            g.add((pom_bnode, RML.graphMap, gm_bnode))
            g.add((gm_bnode, RML[gm_type], RML[gm_value]))
            g.add((gm_bnode, RML.termType, RML.IRI))
    g.serialize(destination=ttl_path, format="turtle")
    return g

rml_df_to_ttl("output_rml.csv", "mapping_generated.ttl")
print(f"Mapping RML generado en: {"mapping_generated.ttl"}")

# Crear un config.ini temporal basado en config original pero con mappings modificado
with tempfile.NamedTemporaryFile(mode="w", suffix=".ini", delete=False) as tmp:
    temp_config_path = tmp.name
    config["DataSource1"]["mappings"] = "mapping_generated.ttl"

    # Guardar el Config modificado en este archivo temporal
    config.write(tmp)

print(f"Config temporal creado: {temp_config_path}")
# 2-3. materializarlo (hacerlo grafo)
graph = morph_kgc.materialize(temp_config_path)

print(f"rdf loaded. Triples number: {len(graph)} (step 2)")

print(f"graph materialized in: {graph} (step 2-3)")

# Convertir el grafo en una lista de triples
triples_data = [(str(s), str(p), str(o)) for s, p, o in graph]

# Crear el DataFrame
df_triples = pd.DataFrame(triples_data, columns=["S", "P", "O"])

# Guardar opcionalmente como CSV
df_triples.to_csv("triples_output.csv", index=False)
print("saved csv file in: triples_output.csv (step 3)")


#opcion2
def extract_bounded_terms(pattern):
    return re.split(r"\{[^}]+\}", pattern)


def filter_df_by_bounded_terms_any_position(df, pattern):
    bounded = extract_bounded_terms(pattern)
    bounded = [b for b in bounded if b.strip()]  # quitar strings vacíos

    def row_matches(row):
        values = [str(row['S']), str(row['P']), str(row['O'])]

        return any(all(b in v for b in bounded) for v in values)

    mask = df.apply(row_matches, axis=1)
    return df[mask]

df = pd.read_csv("triples_output.csv")

#opcion input en codigo
#pattern = "file:///home/jorge/proyectos/git/rdflib-virt/src/pycottas/mapping_generated.ttl#TM35"
#opción input manual
pattern = input("Introduce the pattern to find (S, P u O): ")
filtered = filter_df_by_bounded_terms_any_position(df, pattern)

filtered.to_csv("filtered_templates.csv", index=False)
print("filtered triples in: filtered_templates.csv (step 5)")