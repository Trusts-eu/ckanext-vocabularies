import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
from ckanext.vocabularies.helpers import skos_choices_sparql_helper




class VocabulariesPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IConfigurer)

    # Declare that this plugin will implement ITemplateHelpers.
    plugins.implements(plugins.ITemplateHelpers)

    # IConfigurer

    def update_config(self, config_):
        toolkit.add_template_directory(config_, 'templates')
        toolkit.add_public_directory(config_, 'public')
        toolkit.add_resource('assets',
            'vocabularies')


    def get_helpers(self):
        '''Register the most_popular_groups() function above as a template
        helper function.

        '''
        # Template helper function names should begin with the name of the
        # extension they belong to, to avoid clashing with functions from
        # other extensions.
        return {'skos_vocabulary_helper': skos_choices_sparql_helper}
