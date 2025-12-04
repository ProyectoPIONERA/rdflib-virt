import pandas as pd
from morph_kgc.config import Config
from morph_kgc.mapping.mapping_parser import retrieve_mappings
#from matcher_rf import extract_bound_pattern
from rdflib import Graph
import re

config = Config()
config.read("config.ini")
config.complete_configuration_with_defaults()
config.validate_configuration_section()
rml_df, _, _ = retrieve_mappings(config)
rml_df.to_csv("output_rml.csv", index=False)

print("dataframe generated: output_rml.csv (step 1)")

def rml_df_to_ttl(csv_path, ttl_path):
    """
    Convierte un CSV de rml_df a un mapping Turtle RML formal.
    Genera predicateObjectMap exactamente según object_map_value y predicate_map_value.
    """
    rml = "http://w3id.org/rml/"
    ub = "http://swat.cse.lehigh.edu/onto/univ-bench.owl#"
    rr = "http://www.w3.org/ns/r2rml#"
    rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"


    df = pd.read_csv(csv_path)

    with open(ttl_path, "w", encoding="utf-8") as f:
        # Prefixes comunes
        f.write("@prefix rml: <http://w3id.org/rml/> .\n")
        f.write("@prefix rr: <http://www.w3.org/ns/r2rml#> .\n")
        f.write("@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n")
        f.write("@prefix ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#> .\n\n")

        for _, row in df.iterrows():
            tm_id = row["triples_map_id"]

            # Logical source
            ls_type = str(row.get("logical_source_type", "")).split("/")[-1].lower()
            ls_value = str(row.get("logical_source_value"))
            f.write(f' <{tm_id}> rml:logicalSource [ rml:{ls_type} ')
            if ls_type == "query":
                f.write(f' """{ls_value}""" ; \n            rml:referenceFormulation rml:SQL2008 ] ;\n')
            else : 
                f.write(f' "{ls_value}" ] ;\n')

            # Predicate Object Map
            graph_map_type = str(row.get("graph_map_type", "")).split("/")[-1].lower()
            graph_map_val = str(row.get("graph_map_value")).split("/")[-1].lower()
            f.write("    rml:predicateObjectMap")
            #if "http" in graph_map_type :
            if graph_map_val and graph_map_type != None:
                f.write(f'[ rml:graphMap [ rml:{graph_map_type} rml:{graph_map_val} ;\n                rml:termType rml:IRI ] ;\n')
            else: 
                f.write(f'[ rml:graphMap [ rml:constant rml:defaultGraph ;\n               rml:termType rml:IRI ] ;\n')
                
            # Object Map
            obj_type = str(row.get("object_map_type", "")).split("/")[-1].lower()
            obj_val = row.get("object_map_value")
            if ub in obj_val:
                f.write(f'        rml:objectMap [ rml:{obj_type} ub:{str(obj_val).split("#")[-1].lower()} ;\n                rml:termType rml:IRI ] ;\n')
            else:
                f.write(f'        rml:objectMap [ rml:{obj_type} "{obj_val}" ;\n                rml:termType rml:IRI ] ;\n')

            # Predicate Map
            pred_type = str(row.get("predicate_map_type", "")).split("/")[-1].lower()
            pred_val = row.get("predicate_map_value")
            if rdf in pred_val:
                f.write(f'        rml:predicateMap [ rml:{pred_type} rdf:{str(pred_val).split("#")[-1].lower()} ;\n                rml:termType rml:IRI ] ;\n')
            elif ub in pred_val:
                f.write(f'        rml:predicateMap [ rml:{pred_type} ub:{str(pred_val).split("#")[-1].lower()} ;\n                rml:termType rml:IRI ] ;\n')                

            #Close Predicate object map
            f.write("    ] ;\n")

            # Subject Map
            subj_type = str(row.get("subject_map_type", "")).split("/")[-1].lower()
            subj_val = row.get("subject_map_value")
            f.write(f'    rml:subjectMap [ rml:{subj_type} "{subj_val}" ] .\n\n')

    print(f".ttl mapping genereated in: {ttl_path} (step 1-2)")

rml_df_to_ttl("output_rml.csv", "mapping_generated.ttl")

# Ruta del archivo TTL existente
input_file = "mapping_generated.ttl"   
output_file = "output.ttl"  

# 1. Crear un grafo
graph = Graph()

# 1-2. Cargar el archivo TTL
graph.parse(input_file, format="turtle")

print(f"rdf loaded. Triples number: {len(graph)} (step 2)")

# 2-3. Serializarlo (guardar)
graph.serialize(output_file, format="turtle")

print(f"graph serialized in: {output_file} (step 2-3)")

# Convertir el grafo en una lista de triples
triples_data = [(str(s), str(p), str(o)) for s, p, o in graph]

# Crear el DataFrame
df_triples = pd.DataFrame(triples_data, columns=["S", "P", "O"])

# Guardar opcionalmente como CSV
df_triples.to_csv("triples_output.csv", index=False)
print("saved csv file in: triples_output.csv (step 3)")


"""
#Opcion 1: filtrar usando una función, sacar del matcher los s,p,o y ponerlos en la llamada a la función
def filter_by_pattern(df_triples, s=None, p=None, o=None):
    filtered = df_triples.copy()

    if s and not re.search(r'[\?\$]', s):
        filtered = filtered[filtered["S"].str.contains(re.escape(s))]

    if p and not re.search(r'[\?\$]', p):
        filtered = filtered[filtered["P"].str.contains(re.escape(p))]

    if o and not re.search(r'[\?\$]', o):
        filtered = filtered[filtered["O"].str.contains(re.escape(o))]

    return filtered.reset_index(drop=True)

pattern = extract_bound_pattern("mapping_generated.ttl")

print("\n🔍 DEBUG — Patrón extraído:")
print("Subjects:", pattern["subjects"])
print("Predicates:", pattern["predicates"])
print("Objects:", pattern["objects"])

print("\n📌 DEBUG — Ejemplo de tripletas reales:")
print(df_triples.head(10))

print("\n🎯 Valores bounded detectados:")
print(pattern)

# Por ahora filtramos por el PRIMER bounded encontrado (esto luego se puede mejorar)
s = pattern["subjects"][0] if pattern["subjects"] else None
p = pattern["predicates"][0] if pattern["predicates"] else None
o = pattern["objects"][0] if pattern["objects"] else None

filtered_df = filter_by_pattern(df_triples, s=s, p=p, o=o)

filtered_df.to_csv("filtered_triples.csv", index=False)
print("\n✔ Tripletas filtradas guardadas en: filtered_triples.csv\n")
print(filtered_df)

"""

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