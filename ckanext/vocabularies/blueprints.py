import logging

from flask import Blueprint, request
from ckanext.vocabularies.logic import update_vocabulary_dsc

log = logging.getLogger("ckanext.vocabularies.dsc.subscribe")

vocabularies = Blueprint(
    'vocabularies',
    __name__
)

@vocabularies.route('/vocabularies/actions/update', methods=['GET'])
def update_vocabulary():
    url = request.args.get("url", type=str)
    type = request.args.get("type", type=str)
    if type is not None and url is not None:
        log.info("Updating vocabulary of type %s and url:%s",  type, url)
        if type == "dsc":
            update_vocabulary_dsc(url)
        elif type == "sparql":
            pass
        elif type == "turtle":
            pass
    return "Update complete."
