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

def normalize_predicate_input(p):
    p = p.strip().lower()
    if ":" in p:
        return p.split(":", 1)[1]  # rdf:type → type
    if "#" in p:
        return p.split("#")[-1]    # IRI completa
    return p                      # type

def filter_mapping_by_predicate(rml_df, predicate_input):
    pred_norm = normalize_predicate_input(predicate_input)

    def matches_predicate(iri):
        if not isinstance(iri, str):
            return False
        iri = iri.lower()
        return (
            iri.endswith(f"#{pred_norm}") or
            iri.endswith(f"/{pred_norm}") or
            pred_norm in iri
        )

    mask = rml_df["predicate_map_value"].apply(matches_predicate)
    return rml_df[mask]

pattern = input("Input the predicate of the pattern (?x P ?y), ej: rdf:type, ub:name, type: ")


def rml_df_to_ttl(df, ttl_path):

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
        subj_type = row["subject_map_type"].split("/")[-1].lower()
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
        pred_type = row["predicate_map_type"].split("/")[-1].lower()
        pred_val = row["predicate_map_value"].split("#")[-1].lower()

        g.add((pom_bnode, RML.predicateMap, pred_bnode))
        g.add((pred_bnode, RML[pred_type], UB[pred_val]))

        g.add((pred_bnode, RML.termType, RML.IRI))

        # Object Map ======================================
        obj_bnode = rdflib.BNode()
        obj_type = row["object_map_type"].split("/")[-1].lower()
        obj_value = row["object_map_value"]
        obj_value_short = row["object_map_value"].split("#")[-1].lower()
        obj_termtype = row["object_termtype"].split("/")[-1]

        g.add((pom_bnode, RML.objectMap, obj_bnode))


        if "constant" in obj_type:
            g.add((obj_bnode, RML[obj_type], UB[obj_value_short]))
        elif obj_type in {"reference", "template"}:
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

def extract_bounded_terms(pattern):
    return [b for b in re.split(r"\{[^}]+\}", pattern) if b.strip()]

def filter_df_by_bounded_terms_any_position(df, pattern):
    bounded = extract_bounded_terms(pattern)
    def row_matches(row):
        values = [str(row['S']), str(row['P']), str(row['O'])]
        return any(all(b in v for b in bounded) for v in values)
    mask = df.apply(row_matches, axis=1)
    return df[mask]

config = Config()
config.read("config.ini")
config.complete_configuration_with_defaults()
config.validate_configuration_section()

 # 1. Obtener mapping
rml_df, _ = retrieve_mappings(config)
config.complete_configuration_with_defaults()
config.validate_configuration_section()

# 2. Filtrar TriplesMaps (virtualización real)
rml_df_filtered = filter_mapping_by_predicate(rml_df, pattern)

# 3. Generar mapping TTL (único archivo necesario)
with tempfile.NamedTemporaryFile(suffix=".ttl", delete=False) as ttl_tmp:
    mapping_path = ttl_tmp.name

rml_df_to_ttl(rml_df_filtered, mapping_path)

    # 4. Config temporal
with tempfile.NamedTemporaryFile(mode="w", suffix=".ini", delete=False) as ini_tmp:
    config["DataSource1"]["mappings"] = mapping_path
    config.write(ini_tmp)
    temp_config_path = ini_tmp.name

# 5. Materializar (grafo en memoria)
graph = morph_kgc.materialize(temp_config_path)

# 6. Convertir a DataFrame (en memoria)
df_triples = pd.DataFrame(
    [(str(s), str(p), str(o)) for s, p, o in graph],
    columns=["S", "P", "O"]
    )

# 7. Filtrado final
pattern = input("Introduce el pattern final (S, P u O): ")
filtered = filter_df_by_bounded_terms_any_position(df_triples, pattern)

# 8. (Opcional) guardar resultado
filtered.to_csv("filtered_templates.csv", index=False)

