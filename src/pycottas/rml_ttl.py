import pandas as pd

def rml_csv_to_formal_ttl(csv_path, ttl_path="mapping_rml_formal.ttl"):
    """
    Convierte un CSV generado por rml_df a un mapping Turtle RML formal,
    con rml:logicalSource, rml:predicateObjectMap y rml:subjectMap bien estructurados.
    """
    df = pd.read_csv(csv_path)

    with open(ttl_path, "w", encoding="utf-8") as f:
        # Prefixes RML
        f.write("@prefix rml: <http://w3id.org/rml/> .\n")
        f.write("@prefix rr: <http://www.w3.org/ns/r2rml#> .\n")
        f.write("@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n")
        f.write("@prefix ub: <http://example.com/ontology#> .\n\n")  # sustituye UB por tu ontología

        for _, row in df.iterrows():
            tm = row["triples_map_id"]
            f.write(f"<{tm}> rml:logicalSource [\n")
            f.write(f"        rml:query \"\"\"\n{row['logical_source_value'].strip()}\n        \"\"\" ;\n")
            f.write(f"        rml:referenceFormulation {row['logical_source_type']} ] ;\n")

            # Predicate Object Maps
            f.write("    rml:predicateObjectMap ")

            # Vamos a manejar hasta 3 columnas típicas de object/predicate maps para ejemplo
            pom_blocks = []

            # Bloque 1: tipo
            pom1 = (
                "[ rml:graphMap [ rml:constant rml:defaultGraph ; rml:termType rml:IRI ] ;\n"
                f"  rml:objectMap [ rml:constant {row['object_map_value']} ; rml:termType rml:IRI ] ;\n"
                f"  rml:predicateMap [ rml:constant {row['predicate_map_value']} ; rml:termType rml:IRI ] ]"
            )
            pom_blocks.append(pom1)

            # Aquí podrías agregar más bloques si quieres mapear name, template, etc.
            # Por simplicidad, este ejemplo genera solo un bloque por fila

            f.write(",\n        ".join(pom_blocks) + " ;\n")

            # Subject Map
            f.write(f"    rml:subjectMap [ rml:template \"{row['subject_map_value']}\" ; rml:termType rml:IRI ] .\n\n")

    print(f"✅ Mapping TTL formal generado: {ttl_path}")

rml_csv_to_formal_ttl("output_rml.csv", "mapping_generated.ttl")