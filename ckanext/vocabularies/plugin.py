import ckan.plugins as plugins
from ckanext.vocabularies.helpers import skos_choices_sparql_helper
import ckanext.vocabularies.blueprints as blueprints
import ckan.plugins.toolkit as toolkit
import ckan.model as model
import ckan.logic as logic

class VocabulariesPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IConfigurer)
    # IConfigurer

    def find_or_create_dsc_user(self):
        username = "dsc_user_read"
        password = toolkit.config.get('ckanext.ids.trusts_local_dataspace_connector_password')
        email = "dsc_user_read@example.com"
        user = model.User.by_name(username)
        print("Finding dsc_user_read user")
        if not user:
            print("User not found. Creating one.")
            new_user = {
                "name": username,
                "email": email,
                "password": password
            }
            site_user = logic.get_action(u'get_site_user')({
                u'model': model,
                u'ignore_auth': True},
                {}
            )
            context = {
                u'model': model,
                u'session': model.Session,
                u'ignore_auth': True,
                u'user': site_user['name'],
            }
            toolkit.get_action("user_create")(context, new_user)
            user = model.User.by_name(username)
        else:
            print("User already exists...")

    def update_config(self, config_):
        toolkit.add_template_directory(config_, 'templates')
        toolkit.add_public_directory(config_, 'public')
        toolkit.add_resource('assets',
            'vocabularies')
        SQLALCHEMY_URL = toolkit.config.get('sqlalchemy.url').replace("postgresql", "postgresql+psycopg2")
        toolkit.config.store.update({'ckanext.vocabularies.triplestore': SQLALCHEMY_URL})
        self.find_or_create_dsc_user()

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

    # IBlueprint
    plugins.implements(plugins.IBlueprint)

    def get_blueprint(self):
        return [blueprints.vocabularies]