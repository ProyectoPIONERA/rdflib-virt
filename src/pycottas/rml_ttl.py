import pandas as pd
from rdflib import Graph, URIRef, Literal, Namespace

def rml_df_to_ttl(rml_df, output_file="output.ttl"):
    """
    Convierte un DataFrame RML en un archivo TTL usando rdflib.
    """
    g = Graph()

    # Puedes definir un namespace de ejemplo para las URIs de ejemplo
    EX = Namespace("http://example.org/")

    for _, row in rml_df.iterrows():
        # Crear sujeto
        subject_uri = row['subject_template']
        if '{' in subject_uri:
            # si es template, lo dejamos como URI literal con llaves
            subject = URIRef(subject_uri)
        else:
            subject = URIRef(subject_uri)

        # Crear predicado
        predicate = URIRef(row['predicate'])

        # Crear objeto: si empieza con http, lo tratamos como URI, si no, Literal
        obj_val = row['object_reference']
        if obj_val.startswith("http://") or obj_val.startswith("https://"):
            obj = URIRef(obj_val)
        else:
            obj = Literal(obj_val)

        # Añadir triple al grafo
        g.add((subject, predicate, obj))

    # Guardar el grafo en Turtle
    g.serialize(destination=output_file, format='turtle')
    print(f"✔ Grafo TTL generado en: {output_file}")

# -------------------------
# EJEMPLO DE USO
# -------------------------
if __name__ == "__main__":
    # Supongamos que ya tienes rml_df desde retrieve_mappings
    # Aquí un ejemplo de DataFrame simulado:
    data = {
        "triples_map_id": ["tm1", "tm2"],
        "subject_template": ["http://university.edu/student/{ID}", "http://university.edu/course/{ID}"],
        "predicate": ["http://example.org/name", "http://example.org/title"],
        "object_reference": ["Alice", "Math101"],
        "logical_source": ["students.csv", "courses.csv"]
    }
    rml_df = pd.DataFrame(data)

    rml_df_to_ttl(rml_df, "mappings.ttl")
