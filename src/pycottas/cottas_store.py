__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "julian.arenas.guerrero@upm.es"


import duckdb

from typing import Iterable
from rdflib.store import Store
from rdflib.util import from_n3

from .utils import verify_cottas_file
from .types import Triple
from .tp_translator import translate_triple_pattern_tuple


class COTTASStore(Store):
    """
    - guardar en variable el path al config.ini para después poder pasárselo a morph_kgc al materializar las reglas
    - carga inicial de los mappings con retrieve_mappings(.) -> nos interesa el rml_df
    """
    def __init__(self, path: str, configuration=None, identifier=None):
        super(COTTASStore, self).__init__(configuration=configuration, identifier=identifier)

        if not verify_cottas_file(path):
            raise Exception(f"{path} is not a valid COTTAS file.")

        self._cottas_path = path


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
        for triple in duckdb.execute(translate_triple_pattern_tuple(self._cottas_path, pattern)).fetchall():
            triple = from_n3(triple[0]), from_n3(triple[1]), from_n3(triple[2])
            yield triple, None
        return

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
