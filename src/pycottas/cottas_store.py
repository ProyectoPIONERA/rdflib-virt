__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "julian.arenas.guerrero@upm.es"

import morph_kgc
import tempfile
import duckdb
from typing import Iterable
import rdflib
import pandas as pd
from rdflib.store import Store
from rdflib.util import from_n3
from morph_kgc.mapping.mapping_parser import retrieve_mappings
from morph_kgc.mapping.mapping_parser import MappingParser
from morph_kgc.__init__ import materialize
from morph_kgc.config import Config
from pycottas.utils import verify_cottas_file
from pycottas.types_2 import Triple
from pycottas.tp_translator import translate_triple_pattern_tuple
from pycottas.rml_ttl import rml_df_to_ttl
import re
from pycottas.rml_ttl2 import filter_mapping_by_predicate
from pycottas.rml_ttl2 import rml_df_to_ttl 
from pycottas.rml_ttl2 import filter_df_by_bounded_terms_any_position

config = Config()
config.read("config.ini")
config.complete_configuration_with_defaults()
config.validate_configuration_section()

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

    def triples(self, pattern, final_pattern, context=None):
        """Search for a triple pattern in the mappings and materialize the triples matching the mappings.
        Args:
          - pattern: The triple pattern (s, p, o) to search.
          - context: The query execution context.

        Returns: An iterator that produces RDF triples matching the input triple pattern.
        """
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
        filtered = filter_df_by_bounded_terms_any_position(df_triples, final_pattern)

        # 8. (Opcional) guardar resultado
        filtered.to_csv("filtered_templates.csv", index=False)

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
