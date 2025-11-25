from rdflib import Graph, Namespace
import re

# === Namespaces ===
RML = Namespace("http://w3id.org/rml/")
RR  = Namespace("http://www.w3.org/ns/r2rml#")
RDF = Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#")

OUTPUT_FILE = "resultado_final_matching.txt"

# === Query corregida unificada ===
QUERY = """
SELECT DISTINCT ?tm ?predicateValue ?predType ?object ?objType ?template WHERE { 
    ?tm rml:predicateObjectMap ?pom .
    
    ?pom rml:predicateMap ?pm .
    ?pm ?predType ?predicateValue .

    OPTIONAL {
        ?pom rml:objectMap ?om .
        ?om ?objType ?object .
    }

    OPTIONAL {
        ?tm rml:subjectMap ?sm .
        ?sm rml:template ?template .
    }

    FILTER(?predType IN (rml:constant, rml:template, rml:reference))
    FILTER(!bound(?object) || ?objType IN (rml:constant, rml:template, rml:reference))
}
"""


def normalize_type(value):
    if value is None:
        return "UNKNOWN"
    return value.split("#")[-1] if "#" in value else value


if __name__ == "__main__":

    graph = Graph()
    graph.parse("prueba.ttl", format="turtle")

    rows = list(graph.query(QUERY, initNs={"rml": RML, "rr": RR}))

    # Estructura final: TriplesMap → Subject + Predicates + Objects
    triplesmap_data = {}

    for row in rows:
        tm = str(row.tm)
        predicate = str(row.predicateValue)
        object_val = str(row.object) if row.object else None
        template = str(row.template) if row.template else None
        obj_type = normalize_type(str(row.objType)) if row.objType else None

        if tm not in triplesmap_data:
            triplesmap_data[tm] = {
                "subject": template,
                "predicates": {}
            }

        if predicate not in triplesmap_data[tm]["predicates"]:
            triplesmap_data[tm]["predicates"][predicate] = []

        if object_val:
            triplesmap_data[tm]["predicates"][predicate].append({
                "object": object_val,
                "type": obj_type
            })

    # ---- Escribir salida formateada ----
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:

        f.write("📌 AGRUPACIÓN POR TRIPLESMAP\n")
        f.write("===========================================\n\n")

        for tm, entry in triplesmap_data.items():
            f.write(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
            f.write(f"🔹 TriplesMap: {tm}\n")

            if entry["subject"]:
                f.write(f"    • SubjectMap → {entry['subject']}\n")
            else:
                f.write(f"    • SubjectMap → ❌ No definido\n")

            f.write("\n    Predicates:\n")

            for pred, objects in entry["predicates"].items():
                f.write(f"       🔸 {pred}\n")

                if objects:
                    for obj in objects:
                        f.write(f"          ↳ Object: {obj['object']}  (type: {obj['type']})\n")
                else:
                    f.write("          ↳ ❌ No ObjectMap definido\n")

            f.write("\n")

    print(f"\n📁 Archivo generado correctamente en: {OUTPUT_FILE}\n")
