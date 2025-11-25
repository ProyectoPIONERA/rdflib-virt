import pandas as pd

def rml_df_to_ttl(csv_path, ttl_path="mapping_rml_full.ttl"):
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
        f.write("@prefix ub: <http://example.com/ontology#> .\n\n")

        for _, row in df.iterrows():
            tm_id = row["triples_map_id"]

            # Logical source
            f.write(f"<{tm_id}> rml:logicalSource [")
            f.write(f' rml:query\n """{row["logical_source_value"].strip()}""" ;\n')
            f.write(f"        rml:referenceFormulation rml:SQL2008 ] ;\n")

            # Predicate Object Map
            f.write("    rml:predicateObjectMap [\n")

            # Object Map
            obj_type = row.get("object_map_type", "").lower()
            obj_val = row.get("object_map_value")
            obj_termtype = row.get("object_termtype") if pd.notna(row.get("object_termtype")) else "http://w3id.org/rml/IRI"

            if "constant" in obj_type:
                f.write(f"        rml:objectMap [ rml:constant {obj_val} ; rml:termType {obj_termtype} ]\n")
            elif "template" in obj_type:
                f.write(f"        rml:objectMap [ rml:template {obj_val} ; rml:termType {obj_termtype} ]\n")
            elif "reference" in obj_type:
                f.write(f"        rml:objectMap [ rml:reference {obj_val} ; rml:termType {obj_termtype} ]\n")

            # Predicate Map
            pred_type = row.get("predicate_map_type", "").lower()
            pred_val = row.get("predicate_map_value")
            pred_termtype = "http://w3id.org/rml/IRI"

            f.write(f"        rml:predicateMap [ rml:constant {pred_val} ; rml:termType {pred_termtype} ]\n")
            f.write("    ] ;\n")

            # Subject Map
            f.write(f'    rml:subjectMap [ rml:template "{row["subject_map_value"]}" ; rml:termType rml:IRI ] .\n\n')

    print(f"✅ Mapping TTL limpio generado: {ttl_path}")


rml_df_to_ttl("output_rml.csv", "mapping_generated.ttl")
