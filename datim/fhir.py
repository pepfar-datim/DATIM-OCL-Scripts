"""
Class reqpresenting a FHIR Questionnaire
"""
import json
import requests


class Questionnaire(object):

    def __init__(self, contents, owner='PEPFAR', owner_type='Organization'):
        self._questionnaire = None
        self._owner = owner
        self._owner_type = owner_type
        self.set(contents)

    def set(self, contents):
        self._questionnaire = contents

    @staticmethod
    def load_from_file(filename):
        """ Load Questionnaire from JSON file and return a Questionnaire object """
        with open(filename) as input_file:
            return Questionnaire(json.load(input_file))

    @staticmethod
    def load_from_fhir_server(fhir_server_url='', questionnaire_id=''):
        """ Fetch Questionnaire from FHIR server and return a Questionnaire object """
        # TODO: Enable SSL certificate verification when supported
        questionnaire_request_url = '%sQuestionnaire/?url=%s' % (fhir_server_url, questionnaire_id)
        questionnaire_response = requests.get(questionnaire_request_url, verify=False)
        questionnaire_response.raise_for_status()
        questionnaire = questionnaire_response.json()
        if ('total' in questionnaire and questionnaire['total'] and
                'entry' in questionnaire and
                questionnaire['entry'] and
                questionnaire['entry'][0] and
                'resource' in questionnaire['entry'][0] and
                questionnaire['entry'][0]['resource']):
            return Questionnaire(questionnaire['entry'][0]['resource'])
        return None

    @property
    def owner(self):
        return self._owner
    
    @property
    def owner_type(self):
        return self._owner_type

    @property
    def source(self):
        if self._questionnaire and "name" in self._questionnaire:
            return self._questionnaire["name"]
        return ""

    @property
    def url(self):
        if self._questionnaire and "url" in self._questionnaire:
            return self._questionnaire["url"]
        return ""

    @property
    def title(self):
        if self._questionnaire and "title" in self._questionnaire:
            return self._questionnaire["title"]
        return ""

    def to_json(self):
        """ Get a copy of the questionnaire as JSON """
        return self._questionnaire.copy()
