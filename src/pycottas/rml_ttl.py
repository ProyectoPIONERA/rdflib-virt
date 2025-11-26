import pandas as pd
from morph_kgc.config import Config
from morph_kgc.mapping.mapping_parser import retrieve_mappings

config = Config()
config.read("config.ini")
config.complete_configuration_with_defaults()
config.validate_configuration_section()
rml_df, _, _ = retrieve_mappings(config)
rml_df.to_csv("output_rml.csv", index=False)

print("✔ Archivo generado: output_rml.csv")

def rml_df_to_ttl(csv_path, ttl_path):
    """
    Convierte un CSV de rml_df a un mapping Turtle RML formal.
    Genera predicateObjectMap exactamente según object_map_value y predicate_map_value.
    """
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
            f.write(f"<{tm_id}> rml:logicalSource [")
            f.write(f' rml:query\n """{row["logical_source_value"].strip()}""" ;\n')
            f.write(f"        rml:referenceFormulation rml:SQL2008 ] ;\n")

            # Predicate Object Map
            f.write("    rml:predicateObjectMap [\n")

            # Object Map
            obj_type = str(row.get("object_map_type", "")).split("/")[-1].lower()
            obj_val = row.get("object_map_value")
            f.write(f"        rml:objectMap [ rml:{obj_type} {obj_val} ]\n")

            # Predicate Map
            pred_type = str(row.get("predicate_map_type", "")).split("/")[-1].lower()
            pred_val = row.get("predicate_map_value")
            f.write(f"        rml:predicateMap [ rml:{pred_type} {pred_val} ]\n")
            f.write("    ] ;\n")

            # Subject Map
            subj_type = str(row.get("subject_map_type", "")).split("/")[-1].lower()
            subj_val = row.get("subject_map_value")
            f.write(f"    rml:subjectMap [ rml:{subj_type} {subj_val} ] .\n\n")

    print(f"✅ Mapping TTL limpio generado: {ttl_path}")


rml_df_to_ttl("output_rml.csv", "mapping_generated.ttl")
