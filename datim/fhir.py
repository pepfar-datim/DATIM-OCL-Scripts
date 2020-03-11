"""
Class reqpresenting a FHIR Questionnaire
"""
import json


class Questionnaire(object):

    def __init__(self, contents, owner='PEPFAR', owner_type='Organization'):
        self._questionnaire = None
        self._owner = owner
        self._owner_type = owner_type
        self._source = 'PLM-HIV-Questionnaire'
        self.set(contents)

    def set(self, contents):
        self._questionnaire = contents

    @staticmethod
    def load_from_file(filename):
        with open(filename) as ifile:
            return Questionnaire(json.load(ifile))

    @property
    def owner(self):
        return self._owner
    
    @property
    def owner_type(self):
        return self._owner_type

    @property
    def source(self):
        if self._questionnaire and "questionnaire" in self._questionnaire:
            return self._questionnaire["questionnaire"]["id"]
        return ""

    def to_json(self):
        return self._questionnaire.copy()
