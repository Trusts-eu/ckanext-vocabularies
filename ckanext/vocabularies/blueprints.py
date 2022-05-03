import logging

from flask import Blueprint, request
from flask import Response, stream_with_context
import ckan.plugins.toolkit as toolkit

log = logging.getLogger("ckanext.vocabularies.dsc.subscribe")

vocabularies = Blueprint(
    'vocabularies',
    __name__
)

@vocabularies.route('/vocabularies/actions/update', methods=['POST'])
def push_package():
    return "test"
