__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "julian.arenas.guerrero@upm.es"


import duckdb
from typing import Iterable
from rdflib import Graph
import rdflib
import pandas as pd
from rdflib.store import Store
from rdflib.util import from_n3
from morph_kgc.mapping.mapping_parser import retrieve_mappings
from morph_kgc.mapping.mapping_parser import MappingParser
from morph_kgc.__init__ import materialize
from .utils import verify_cottas_file
from .types import Triple
from .tp_translator import translate_triple_pattern_tuple
from .rml_ttl import rml_df_to_ttl
import re


config_path = "config.ini"

class COTTASStore(Store):
    """
    - guardar en variable el path al config.ini para después poder pasárselo a morph_kgc al materializar las reglas
    - carga inicial de los mappings con retrieve_mappings(.) -> nos interesa el rml_df
    """

    def __init__(self, path: str, configuration=None, identifier=None):
        super().__init__(configuration=configuration, identifier=identifier)

        if not verify_cottas_file(path):
            raise Exception(f"{path} is not a valid COTTAS file.")

        self.config_path = path

    def triples(self, pattern, context) -> Iterable[Triple]:
        """Search for a triple pattern in the mappings and materialize the triples matching the mappings.

        1. Matchear el *pattern* con rml_df -> obtiene un rml_df con menos reglas
        2. Ejecutar morph_kgc el rml_df que se ha macheado -> obtiene un grafo de RDFlib
        3. A partir del grafo de rdflib anterior crear un dataframe de tripletas con columnas S, P, O (esto no se debería hacer pero nos vale de momento)
        4. Filtrar el dataframe de tripletas con el pattern (utilizando los términos qu estén bounded, es decir, los que no son variables)
        5. Devolver las tripletas (yield triples)

        Args:
          - pattern: The triple pattern (s, p, o) to search.
          - context: The query execution context.

        Returns: An iterator that produces RDF triples matching the input triple pattern.
        """

        parser = MappingParser(self.config_path)
        self.config = parser.get_config()
        #1. Retrieve mappings to get rml_df filtered
        rml_df, _, _ = retrieve_mappings(self.config)

        #2. transform rml_df filtered to a .ttl and materialize to get a graph
        rml_df_ttl = rml_df_to_ttl(rml_df, "rml_turtled.ttl")
        rdf_graph = materialize(rml_df_ttl, config=self.config)

        #3. convert graph into dataframe
        triples = [
            {"S": str(s), "P": str(p), "O": str(o)}
            for s, p, o in rdf_graph.triples((None, None, None))
        ]       

        df = pd.DataFrame(triples)

        #4. Filter dataframe with pattern (bounded terms)
        def filtered_triples(df: pd.DataFrame, pattern: str) -> pd.DataFrame:

            def is_bound(term):
                # Variable if has {, }, $, ?
                return not re.search(r'[\{\}\$\?]', term)
            
            mask = df.apply(lambda row: any(
                is_bound(row[col]) and re.search(pattern, row[col])
                for col in ['S','P','O']
            ), axis=1)

            return df[mask].reset_index(drop=True)
        
        filtered_df = filtered_triples(df)

        # 5. Yield triples filtered
        for _, row in filtered_df.iterrows():
            yield (
                from_n3(row["S"]),
                from_n3(row["P"]),
                from_n3(row["O"])
            ), None

    def create(self, configuration):
        raise TypeError('The virt store is read only!')

    def destroy(self, configuration):
        raise TypeError('The virt store is read only!')

    def commit(self):
        raise TypeError('The virt store is read only!')

    def rollback(self):
        raise TypeError('The virt store is read only!')

    def add(self, _, context=None, quoted=False):
        raise TypeError('The virt store is read only!')

    def addN(self, quads):
        raise TypeError('The virt store is read only!')

    def remove(self, _, context):
        raise TypeError('The virt store is read only!')
