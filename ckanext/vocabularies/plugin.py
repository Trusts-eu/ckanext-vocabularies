import ckan.plugins as plugins
from ckan.common import config
import ckan.plugins.toolkit as toolkit
from ckanext.vocabularies.helpers import skos_choices_sparql_helper
import ckan.plugins.toolkit as toolkit


class VocabulariesPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IConfigurer)
    # IConfigurer

    def update_config(self, config_):
        toolkit.add_template_directory(config_, 'templates')
        toolkit.add_public_directory(config_, 'public')
        toolkit.add_resource('assets',
            'vocabularies')
        SQLALCHEMY_URL = toolkit.config.get('sqlalchemy.url').replace("postgresql", "postgresql+psycopg2")
        toolkit.config.store.update({'ckanext.vocabularies.triplestore': SQLALCHEMY_URL})

    # Declare that this plugin will implement ITemplateHelpers.
    plugins.implements(plugins.ITemplateHelpers)

    def get_helpers(self):
        '''Register the skos_vocabulary_helper() function as a template
        helper function.
        '''
        # Template helper function names should begin with the name of the
        # extension they belong to, to avoid clashing with functions from
        # other extensions.
        return {'skos_vocabulary_helper': skos_choices_sparql_helper}