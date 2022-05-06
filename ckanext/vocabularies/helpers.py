from SPARQLWrapper import SPARQLWrapper, JSON
import requests
import json
import logging

import ckan.plugins.toolkit as toolkit
from rdflib import plugin, URIRef
from rdflib.graph import Graph
from rdflib.store import Store
from rdflib_sqlalchemy import registerplugins

from ckanext.vocabularies.dsc.subscribe import Subscription

log = logging.getLogger("ckanext.vocabularies")



# deprecated
## create local triplestore on postgresql
registerplugins()
store = plugin.get("SQLAlchemy", Store)(identifier="vocabularies_store")

# keeps graphs in memory
graphs = dict()


def query_with_in_scheme(concept_scheme, lang="en"):
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
                FILTER ( langMatches( lang(?prefLabel), "%(lang)s") && langMatches( lang(?broaderPrefLabel), "%(lang)s"))       
                }
                group by ?concept ?prefLabel
            """%dict(in_scheme_triple=in_scheme_triple, lang=lang)


def query_poolparty(sparql_endpoint, query):
    response = requests.post(sparql_endpoint, data={"query": query, "format": "application/json"}, headers={ "Content-Type": "application/x-www-form-urlencoded"}).content.decode("utf-8")
    return json.loads(response)


def query_public_endpoint(sparql_endpoint, query):
    sparql = SPARQLWrapper(sparql_endpoint)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    return sparql.queryAndConvert()


def format_results(results):
    skos_choices = []
    if results['results']['bindings']:
        bindings = results['results']['bindings']
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
        log.info('reformatted results')
        return sorted(skos_choices, key=lambda d: d['label'])
    else:
        return skos_choices.append({'value': '', 'label': 'empty'})


def query_dsc_resource(dsc_resource, dsc_resource_contract,query):
    if dsc_resource in graphs:
        graph = graphs[dsc_resource]
        log.info('Graph exists in triplestore...querying')
    else:
        log.info('Graph does not exist on triplestore...adding triples')
        graph = Graph()
        subscription = Subscription(dsc_resource, dsc_resource_contract)
        subscription.make_agreement()
        data = subscription.consume_resource()

        graph.parse(data=data, format="text/turtle")
        graphs[dsc_resource] = graph
    results = json.loads(graph.query(query).serialize(format='json'))
    return results

# deprecated
def query_dsc_resource_rdbms(dsc_resource, query):
    graph = Graph(store, identifier=dsc_resource)
    try:
        graph.open(URIRef(toolkit.config.get('ckanext.vocabularies.triplestore')), create=False)
        log.info('Graph exists in triplestore...querying')
    except RuntimeError as error:
        log.info('Graph does not exist on triplestore...adding triples')
        print(error)
        graph = load_triples(dsc_resource, graph)
    results = json.loads(graph.query(query).serialize(format='json'))
    graph.close()
    return results

# deprecated, keeping in case we need to use external triplestore
def load_triples(dsc_resource, graph):
    log.info('Opening store')
    graph.open(URIRef(toolkit.config.get('ckanext.vocabularies.triplestore')), create=True)
    log.info('Adding triples')
    graph.parse("test.ttl", format="text/turtle")
    log.info('Finished adding triples')
    return graph


def skos_choices_sparql_helper(field):
    '''Return a list of the concepts of a concept scheme served from a SPARQL endpoint'''

    sparql_endpoint = field.get('skos_choices_sparql_endpoint', None)
    is_poolparty = field.get('skos_choices_is_poolparty', False)
    concept_scheme = field.get('skos_choices_concept_scheme', None)
    dsc_resource = field.get('skos_choices_dsc_resource', None)
    dsc_resource_contract = field.get('skos_choices_dsc_resource_contract_offer', None)

    query = query_with_in_scheme(concept_scheme)
    if dsc_resource is not None:
        log.info('querying dsc')
        results = query_dsc_resource(dsc_resource, dsc_resource_contract, query)
    elif is_poolparty:
        results = query_poolparty(sparql_endpoint, query)
    else:
        results = query_public_endpoint(sparql_endpoint, query)

    log.info('received results')
    return format_results(results)
