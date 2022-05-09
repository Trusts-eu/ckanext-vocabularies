import logging

import ckan.plugins.toolkit as toolkit

import pathlib
from ckanext.ids.model import IdsResource, IdsAgreement, IdsSubscription
from urllib.parse import urlparse
from ckanext.vocabularies.dsc.resourceapi import ResourceApi
from ckanext.vocabularies.dsc.subscriptionapi import SubscriptionApi
from ckanext.vocabularies.dsc.idsapi import IdsApi

log = logging.getLogger("ckanext.vocabularies.dsc.subscribe")

consumerUrl = toolkit.config.get('ckanext.ids.trusts_local_dataspace_connector_url') + ":" + toolkit.config.get('ckanext.ids.trusts_local_dataspace_connector_port')
username = toolkit.config.get('ckanext.ids.trusts_local_dataspace_connector_username')
password = toolkit.config.get('ckanext.ids.trusts_local_dataspace_connector_password')
consumer_alias = consumerUrl

consumer = IdsApi(consumerUrl, auth=(username, password))
local_node = toolkit.config.get("ckanext.ids.trusts_local_dataspace_connector_url")


class Subscription:
    offer_url = None
    contract_url = None
    provider_alias = None
    agreement_url = None
    first_artifact = None
    remote_artifact = None
    data_source = None
    endpoint = None
    route = None


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
            local_subscription = local_agreement.get_subscriptions()
            if len(local_subscription) == 0:
                self.subscribe()
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
        self.subscribe()

    def consume_resource(self):
        log.info("Consuming resource...")
        consumerResources = ResourceApi(consumerUrl)
        artifacts = consumerResources.get_artifacts_for_agreement(self.agreement_url)
        log.debug(artifacts)
        first_artifact = artifacts["_embedded"]["artifacts"][0]["_links"]["self"]["href"]
        self.first_artifact = first_artifact
        log.debug(first_artifact)
        data = consumerResources.get_data(first_artifact).text
        log.info("Data acquired successfully!")
        #log.debug(data)
        return data
# Consumer

    def subscribe(self):
        self.create_data_source()
        self.create_endpoint()
        self.link_endpoint_data_source()
        self.create_route()
        self.link_endpoint_route()
        local_agreement = IdsAgreement.get(self.agreement_url)
        consumerResources = ResourceApi(consumerUrl)
        artifacts = consumerResources.get_artifacts_for_agreement(self.agreement_url)
        log.debug(artifacts)
        first_artifact = artifacts["_embedded"]["artifacts"][0]["_links"]["self"]["href"]
        self.first_artifact = first_artifact

        # subscribe to the requested artifact
        consumerSub = SubscriptionApi(consumerUrl)
        data = {
            "title": "CKAN on " + local_node + "vocabulary subscription",
            "description": "",
            "target": self.first_artifact.replace(local_node + ":8282", "http://locahlost:8080"),
            "location": self.route.replace(local_node + ":8282", "http://localhost:8080"),
            "subscriber":  local_node + ":5000",
            "pushData": "false",
        }

        response = consumerSub.create_subscription(data=data)
        local_subscription = IdsSubscription(id=response, agreement=local_agreement, user=username)
        local_subscription.save()
        log.debug(response)

        ## this is used to create ids subscription
        ## subscribe to the remote offer
        data = {
            "title": "DSC on " + local_node + "vocabulary subscription",
            "description": "",
            "target": self.remote_artifact,
            "location": consumerUrl + "/api/ids/data",
            "subscriber": consumerUrl,
            "pushData": "true",
        }
        response = consumerSub.subscription_message(
            data=data, params={"recipient": self.provider_alias + "/api/ids/data"}
        )

    def create_data_source(self):
        data = {
            "authentication": {
                "key": "dsc_user_read",
                "value": toolkit.config.get('ckanext.ids.trusts_local_dataspace_connector_password')
            },
            "type": "REST"
        }
        data_source = consumer.create_data_source(data)
        self.data_source = data_source["id"]
        return data_source

    def create_endpoint(self):
        data = {
            "location": "http://local-ckan:5000/vocabularies/actions/update?type=dsc&url=" + self.offer_url,
            "type": "GENERIC"
        }
        endpoint = consumer.create_endpoint(data)
        self.endpoint = pathlib.PurePath(endpoint["_links"]["self"]["href"]).name
        return endpoint

    def link_endpoint_data_source(self):
        response = consumer.link_endpoint_datasource(self.endpoint, self.data_source)
        return response

    def create_route(self):
        data = {
            "title": "Vocabulary Update Route",
            "deploy": "Camel"
        }
        response = consumer.create_route(data)
        self.route = response["_links"]["self"]["href"]
        return response

    def link_endpoint_route(self):
        response = consumer.link_route_endpoint(pathlib.PurePath(self.route).name, self.endpoint)
        return response