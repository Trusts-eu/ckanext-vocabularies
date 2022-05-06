import logging
from ckanext.ids.model import IdsResource, IdsAgreement
from ckanext.vocabularies.dsc.subscribe import Subscription
from ckanext.vocabularies.helpers import graphs
from rdflib import Graph

log = logging.getLogger("ckanext.vocabularies.logic")


def update_vocabulary_dsc(dsc_resource):
    local_resource = IdsResource.get(dsc_resource)
    local_agreement = local_resource.get_agreements()[0]
    log.info("Found the agreement url:" + local_agreement.id)
    subscription = Subscription(dsc_resource, None)
    subscription.agreement_url = local_agreement.id
    data = subscription.consume_resource()
    graph = Graph()
    graph.parse(data=data, format="text/turtle")
    log.info("Updating graph in local store...")
    graphs[dsc_resource] = graph
    log.info("Finished")
