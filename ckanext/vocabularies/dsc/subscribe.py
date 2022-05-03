import pprint
import sys
import logging

import ckan.plugins.toolkit as toolkit

from ckanext.ids.model import IdsResource, IdsAgreement
from urllib.parse import urlparse
from ckanext.vocabularies.dsc.resourceapi import ResourceApi
from ckanext.vocabularies.dsc.subscriptionapi import SubscriptionApi
from ckanext.vocabularies.dsc.idsapi import IdsApi

log = logging.getLogger("ckanext.vocabularies.dsc.subscribe")

#providerUrl = "http://34.77.70.203:8181"
consumerUrl = toolkit.config.get('ckanext.ids.trusts_local_dataspace_connector_url') + ":" + toolkit.config.get('ckanext.ids.trusts_local_dataspace_connector_port')
username = toolkit.config.get('ckanext.ids.trusts_local_dataspace_connector_username')
password = toolkit.config.get('ckanext.ids.trusts_local_dataspace_connector_password')
consumer_alias = consumerUrl

consumer = IdsApi(consumerUrl, auth=(username, password))

class Subscription:
    offer_url = None
    contract_url = None
    provider_alias = None
    agreement_url = None
    first_artifact = None
    remote_artifact = None

    def __init__(self, offer_url, contract_url):
        self.offer_url = offer_url
        self.contract_url = contract_url

        parsed_url = urlparse(offer_url)
        self.provider_alias = parsed_url.scheme + "://" + parsed_url.netloc

# IDS
# Call description
    def make_agreement(self):
        log.info("Making agreement...")
        offer = consumer.descriptionRequest(self.provider_alias + "/api/ids/data", self.offer_url)
        log.debug(offer)
        artifact = offer["ids:representation"][0]["ids:instance"][0]['@id']
        self.remote_artifact = artifact
        log.debug(artifact)
        ## Check if an agreement already exists
        local_resource = IdsResource.get(self.offer_url)
        if local_resource is not None:
            local_agreement = local_resource.get_agreements()[0]
            self.agreement_url = local_agreement.id
            log.info("Agreement was made before... getting agreement url:%s", self.agreement_url)
            return
        # else Negotiate contract
        obj = offer["ids:contractOffer"][0]["ids:permission"][0]
        obj["ids:target"] = artifact
        log.debug(obj)
        response = consumer.contractRequest(self.provider_alias + "/api/ids/data", self.offer_url, artifact, False, obj)
        log.debug(response)
        self.agreement_url = response["_links"]["self"]["href"]
        log.info("Agreement made... agreement url:%s", self.agreement_url)
        local_resource = IdsResource(self.offer_url)
        local_resource.save()
        local_agreement = IdsAgreement(id=self.agreement_url,
                                   resource=local_resource,
                                   user="admin")
        local_agreement.save()

    def consume_resource(self):
        log.info("Consuming resource...")
        consumerResources = ResourceApi(consumerUrl)
        artifacts = consumerResources.get_artifacts_for_agreement(self.agreement_url)
        log.debug(artifacts)
        first_artifact = artifacts["_embedded"]["artifacts"][0]["_links"]["self"]["href"]
        log.debug(first_artifact)
        data = consumerResources.get_data(first_artifact).text
        log.info("Data acquired successfully!")
        #log.debug(data)
        return data
# Consumer

    def subscribe(self):
        # subscribe to the requested artifact
        consumerSub = SubscriptionApi(consumerUrl)
        data = {
            "title": "CKAN SWC core vocabulary subscription",
            "description": "string",
            "target": self.first_artifact,
            "location": "https://dsc-subscriber.requestcatcher.com/test",
            "subscriber": "http://swc-core:5000",
            "pushData": "true",
        }

        response = consumerSub.create_subscription(data=data)
        log.debug(response)

        ## this is used to create ids subscription
        ## subscribe to the remote offer
        data = {
            "title": "string",
            "description": "string",
            "target": self.artifact,
            "location": consumerUrl + "/api/ids/data",
            "subscriber": consumerUrl,
            "pushData": "true",
        }
        response = consumerSub.subscription_message(
            data=data, params={"recipient": self.provider_alias + "/api/ids/data"}
        )