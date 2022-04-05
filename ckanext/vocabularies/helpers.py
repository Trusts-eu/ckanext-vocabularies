from rdflib import Graph
from SPARQLWrapper import SPARQLWrapper, XML, JSON
import requests
import json


def query_with_in_scheme(concept_scheme):
    if concept_scheme != None:
        in_scheme_triple = "?concept skos:inScheme <%(concept_scheme)s>"%dict(concept_scheme=concept_scheme)
    else: in_scheme_triple = ''
    return """
            PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
            select ?concept ?prefLabel (GROUP_CONCAT(?broaderPrefLabel;SEPARATOR="|") as ?path) (count(?broader) as ?counter) 
            where {
                ?concept a skos:Concept ;
                         skos:prefLabel ?prefLabel .
                %(in_scheme_triple)s         
                OPTIONAL { ?concept skos:broader+ ?broader .
                           ?broader skos:prefLabel ?broaderPrefLabel .
                        }
                }
                group by ?concept ?prefLabel
            """%dict(in_scheme_triple=in_scheme_triple)

def skos_choices_sparql_helper(field):
    '''Return a list of the concepts of a concept scheme served from a SPARQL endpoint'''

    sparql_endpoint = field['skos_choices_sparql_endpoint']
    is_poolparty = field.get('skos_choices_is_poolparty', False)
    concept_scheme = field.get('skos_choices_concept_scheme', None)

    query = query_with_in_scheme(concept_scheme)
    if is_poolparty:
        response = requests.post(sparql_endpoint, data={"query": query, "format": "application/json"}, headers={ "Content-Type": "application/x-www-form-urlencoded"}).content.decode("utf-8")
        results = json.loads(response)
    else:
        sparql = SPARQLWrapper(sparql_endpoint)
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        try:
            results = sparql.queryAndConvert()
        except Exception as e:
            print(e)
    if results['results']['bindings']:
        bindings = results['results']['bindings']
        skos_choices = []
        for binding in bindings:
            path = (binding['path']['value'].split("|"))
            prefLabel = binding['prefLabel']['value']
            if path[0] != '':
                path.reverse()
                path.append(prefLabel)
                label = ' -> '.join(path)
            else:
                label = prefLabel
            choice = {
                'value': binding['concept']['value'],
                'label': label
            }
            skos_choices.append(choice)
    ordered_choices = sorted(skos_choices, key=lambda d: d['label'])
    return ordered_choices
