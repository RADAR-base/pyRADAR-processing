#!/usr/bin/env python3
import requests
from requests_oauthlib import OAuth2Session
from oauthlib.oauth2 import BackendApplicationClient


class Resource():
    """ A generic resource object to form part of the
    ManagementPortal object. Sends requests using the
    ManagementPortal.request method
    """
    _name = ''

    def __init__(self, portal):
        self._portal = portal

    def request(self, method, endpoint='', **kwargs):
        endpoint = self._name + '/' + endpoint if endpoint else self._name
        return self._portal.request(method, endpoint, **kwargs)


class Subject(Resource):
    _name = 'subjects'

    def get(self, login='', **params):
        return self.request('GET', endpoint=login,
                            params=params)

    def get_all(self, **params):
        return self.get('', **params)

    def create(self, **body):
        return self.request('POST', json=body)

    def update(self, **body):
        return self.request('PUT', json=body)

    def discontinue(self, **body):
        return self.request('PUT', endpoint='discontinue', json=body)

    def delete(self, login):
        return self.request('DELETE', login)

    def get_sources(self, login):
        return self.request('GET', login + '/sources')

    def assign_sources(self, login, **body):
        if not (('sourceTypeId' in body) or
                any(x in body for x in ('sourceTypeProducer',
                                        'sourceTypeModel',
                                        'sourceTypeCatalogVersion'))):
            raise ValueError(('You must supply either a Source Type ID, or the'
                              'combination of (sourceTypeProducer,'
                              'sourceTypeModel, sourceTypeCatalogVersion)'))
        return self.request('POST', endpoint=login, json=body)

    def revisions(self, login):
        return self.request('GET', endpoint=login + '/revisions')


class Project(Resource):
    _name = 'projects'

    def get(self, project_name='', **params):
        return self.request('GET', endpoint=project_name,
                            params=params if not project_name else None)

    def get_all(self, **params):
        return self.get('', **params)

    def create(self, project_name, description, location, **body):
        body['projectName'] = project_name
        body['description'] = description
        body['location'] = location
        return self.request('POST', json=body)

    def update(self, project_name, description, location, **body):
        body['projectName'] = project_name
        body['description'] = description
        body['location'] = location
        return self.request('PUT', json=body)

    def delete(self, project_name):
        return self.request('DELETE', project_name)

    def get_roles(self, project_name):
        endpoint = project_name + '/roles'
        return self.request('GET', endpoint)

    def get_source_types(self, project_name):
        endpoint = project_name + '/source-types'
        return self.request('GET', endpoint)

    def get_sources(self, project_name, **params):
        endpoint = project_name + '/sources'
        return self.request('GET', endpoint, params=params)

    def get_subjects(self, project_name, **params):
        endpoint = project_name + '/subjects'
        return self.request('GET', endpoint, params=params)


class Source(Resource):
    _name = 'sources'

    def get(self, source_name='', **params):
        return self.request('GET', endpoint=source_name,
                            params=params if not source_name else None)

    def get_all(self, **params):
        return self.get('', **params)

    def create(self, source_name, source_type, assigned, **body):
        body['assigned'] = assigned
        body['sourceName'] = source_name
        body['sourceType'] = source_type
        return self.request('POST', json=body)

    def update(self, source_name, source_type, assigned, **body):
        body['assigned'] = assigned
        body['sourceName'] = source_name
        body['sourceType'] = source_type
        return self.request('PUT', json=body)

    def delete(self, source_name):
        return self.request('DELETE', source_name)


class SourceType(Resource):
    _name = 'source-types'

    def get(self, producer='', model='', version='', **params):
        if model and not producer:
            raise ValueError('You must specify a producer to specify a model')
        if version and not model:
            raise ValueError('You must specify a model to specify a version')
        endpoint = '/'.join([x for x in (producer, model, version) if x])
        return self.request('GET', endpoint=endpoint,
                            params=params if not model else None)

    def get_all(self, **params):
        return self.get(**params)

    def create(self, producer, model, version, can_register_dynamically, scope,
               **body):
        body['producer'] = producer
        body['model'] = model
        body['catalogVersion'] = version
        body['canRegisterDynamically'] = can_register_dynamically
        body['sourceTypeScope'] = scope
        return self.request('POST', json=body)

    def update(self, producer, model, version, can_register_dynamically, scope,
               **body):
        body['producer'] = producer
        body['model'] = model
        body['catalogVersion'] = version
        body['canRegisterDynamically'] = can_register_dynamically
        body['sourceTypeScope'] = scope
        return self.request('PUT', json=body)

    def delete(self, producer, model, version):
        endpoint = '/'.join([producer, model, version])
        return self.request('DELETE', endpoint)


class SourceData(Resource):
    _name = 'source-data'

    def get(self, source_data_name='', **params):
        return self.request('GET', source_data_name,
                            params=params if not source_data_name else None)

    def get_all(self, **params):
        return self.get(**params)

    def create(self, source_data_type, **body):
        body['sourceDataType'] = source_data_type
        return self.request('POST', json=body)

    def update(self, source_data_type, **body):
        body['sourceDataType'] = source_data_type
        return self.request('PUT', json=body)

    def delete(self, source_data_name):
        return self.request('DELETE', source_data_name)


class ManagementPortal():
    """ A class to call resource REST API requests from. Requires
    a RADAR management portal API account.
    """

    def __init__(self, client_id, client_secret, url):
        self._url = url.rstrip('/')
        self.s = requests.Session()
        self.s.headers['Authorization'] = \
            'Bearer {}'.format(self.authenticate(client_id, client_secret))
        self.subjects = Subject(portal=self)
        self.projects = Project(portal=self)
        self.source = Source(portal=self)
        self.source_type = SourceType(portal=self)
        self.source_data = SourceData(portal=self)

    def authenticate(self, client_id, client_secret):
        def get_token(client_id, client_secret, oauth, url):
            token_url = url + '/oauth/token'
            token = oauth.fetch_token(token_url=token_url,
                                      client_id=client_id,
                                      client_secret=client_secret)
            return token
        client = BackendApplicationClient(client_id=client_id)
        oauth = OAuth2Session(client=client)
        token = get_token(client_id, client_secret, oauth, self._url)
        return token['access_token']

    def request(self, method, endpoint, **kwargs):
        url = self._url + '/api/' + endpoint
        r = self.s.request(method, url, **kwargs)
        if (r.status_code == 401 and 'error_description' in r.json() and
                'Access token expired' in r.json()['error_description']):
            err = ('401 Client Error: Access token has expired')
            raise requests.HTTPError(err)
        r.raise_for_status()
        return r
