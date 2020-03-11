"""
Script to import a QMAP into OCL. A QMAP is a JSON representation of a mapping between
a point of service system (POS) to a FHIR Questionnaire.
"""
import re
import json
import pprint
import requests
import ocldev.oclconstants
import ocldev.oclexport
import ocldev.oclvalidator
import ocldev.oclresourcelist
import ocldev.oclfleximporter
import fhir


class Qmap(object):
    """ Class for a QMAP object """

    # QMAP Version
    QMAP_VERSION = '1.0.0'

    # QMAP Concept Class constants
    QMAP_CONCEPT_CLASS_POS_HEADER = 'Header'
    QMAP_CONCEPT_CLASS_POS_CHOICE = 'Choice'
    QMAP_CONCEPT_CLASS_POS_CONSTANT = 'Constant'

    def __init__(self, qmap_json=None):
        """ Initialize Qmap object """
        self._qmap = qmap_json

    def __str__(self):
        """ Get string representation of Qmap object """
        return json.dumps(self._qmap)

    @staticmethod
    def export_qmap(domain='', qmap_id='', ocl_env_url='', ocl_api_token='', verbosity=0):
        """ Export Qmap from OCL """

        # Verify that the specified domain/qmap_id exist as org/source in OCL
        try:
            if not Qmap.check_if_qmap_source_exists(
                    domain=domain, qmap_id=qmap_id,
                    ocl_env_url=ocl_env_url, ocl_api_token=ocl_api_token):
                err_msg = 'Source "%s" for domain "%s" on "%s" is not compatible with this QMAP service' % (
                    qmap_id, domain, ocl_env_url)
                raise Exception(err_msg)
        except:
            err_msg = 'Org "%s" or source "%s" does not exist on "%s" or cannot be accessed' % (
                domain, qmap_id, ocl_env_url)
            raise Exception(err_msg)

        # Retrieve qmap export from OCL
        qmap_repo_url = '%s%s' % (ocl_env_url, ocldev.oclconstants.OclConstants.get_repository_url(
            owner_id=domain, repository_id=qmap_id, include_trailing_slash=True))
        if verbosity:
            print 'Requesting export from: %s ' % qmap_repo_url
        qmap_export = ocldev.oclexport.OclExportFactory.load_latest_export(
            qmap_repo_url, oclapitoken=ocl_api_token)
        if not qmap_export:
            err_msg = "Unable to load QMAP export from OCL for domain '%s' and qmap_id '%s'" % (
                domain, qmap_id)
            raise Exception(err_msg)

        # Build QMAP JSON
        pprint.pprint(Qmap.build_qmap_from_ocl_export(qmap_export=qmap_export))

    @staticmethod
    def check_if_qmap_source_exists(domain='', qmap_id='', ocl_env_url='', ocl_api_token=''):
        qmap_repo_url = '%s%s' % (ocl_env_url, ocldev.oclconstants.OclConstants.get_repository_url(
            owner_id=domain, repository_id=qmap_id, include_trailing_slash=True))
        ocl_api_headers = {'Content-Type': 'application/json'}
        if ocl_api_token:
            ocl_api_headers['Authorization'] = 'Token ' + ocl_api_token
        response = requests.get(qmap_repo_url, headers=ocl_api_headers)
        try:
            response.raise_for_status()
        except:
            return False
        qmap_source = response.json()
        if "extras" in qmap_source and "qmap_version" in qmap_source["extras"]:
            return True
        raise Exception('Source "%s" exists but is not configured as a QMAP source' % (
            qmap_repo_url))

    @staticmethod
    def check_if_org_exists(domain='', ocl_env_url='', ocl_api_token=''):
        qmap_org_url = '%s%s' % (ocl_env_url, ocldev.oclconstants.OclConstants.get_owner_url(
            domain, include_trailing_slash=True))
        ocl_api_headers = {'Content-Type': 'application/json'}
        if ocl_api_token:
            ocl_api_headers['Authorization'] = 'Token ' + ocl_api_token
        response = requests.get(qmap_org_url, headers=ocl_api_headers)
        try:
            response.raise_for_status()
        except Exception:
            return False
        return True

    @staticmethod
    def build_qmap_from_ocl_export(qmap_export=None):
        _qmap_export_json = qmap_export.get_full_export()
        _qmap_contents = {
            "name": _qmap_export_json["source"]["name"],
            "created": _qmap_export_json["source"]["created_on"],
            "updated": _qmap_export_json["source"]["updated_on"],
            "uid": _qmap_export_json["source"]["external_id"],
            "questionnaireuid": _qmap_export_json["source"]["extras"]["questionnaireuid"],
            "complete": True,
            "map": {
                "headers": {},
                "constants": {},
            }
        }

        # Process headers
        for _header_concept in qmap_export.get_concepts(
                core_attrs={"concept_class": Qmap.QMAP_CONCEPT_CLASS_POS_HEADER}):
            _qmap_contents["map"]["headers"][_header_concept["id"]] = {
                "path": _header_concept["extras"]["path"],
                "valueType": _header_concept["datatype"]
            }

        # Process choices
        for _choice_concept in qmap_export.get_concepts(
                core_attrs={"concept_class": Qmap.QMAP_CONCEPT_CLASS_POS_CHOICE}):
            _linked_header_concept_id = _choice_concept["extras"]["header_concept_id"]
            if _linked_header_concept_id not in _qmap_contents["map"]["headers"]:
                # Possibly throw exception in the future, but for now just skip
                continue
            elif "choiceMap" not in _qmap_contents["map"]["headers"][_linked_header_concept_id]:
                _qmap_contents["map"]["headers"][_linked_header_concept_id]["choiceMap"] = {}
            _qmap_contents["map"]["headers"][_linked_header_concept_id]["choiceMap"][
                _choice_concept['display_name']] = (
                    _choice_concept["extras"]["questionnaire_choice_value"])

        # Process constants
        for _constant_concept in qmap_export.get_concepts(
                core_attrs={"concept_class": Qmap.QMAP_CONCEPT_CLASS_POS_CONSTANT}):
            _constant_node_key = _constant_concept["extras"]["path"][-1:][0]['linkid']
            _qmap_contents["map"]["constants"][_constant_node_key] = {
                "display": Qmap.get_concept_name_by_type(_constant_concept, "display"),
                "code": Qmap.get_concept_name_by_type(_constant_concept, "code"),
                "valueType": _constant_concept["datatype"],
                "path": _constant_concept["extras"]["path"]
            }

        return _qmap_contents

    @staticmethod
    def get_concept_name_by_type(concept, name_type):
        for concept_name in concept["names"]:
            if concept_name["name_type"] == name_type:
                return concept_name["name"]
        return ""

    def import_qmap(self, domain='', ocl_env_url='', ocl_api_token='', test_mode=False,
                    verbosity=0):
        """ Import Qmap into OCL """

        # Determine if we need to create a new org
        does_org_exist = False
        if Qmap.check_if_org_exists(
                domain=domain, ocl_env_url=ocl_env_url, ocl_api_token=ocl_api_token):
            does_org_exist = True
        do_create_org = not does_org_exist

        # Determine if the source already exists
        does_source_exist = False
        do_create_source = True
        if Qmap.check_if_qmap_source_exists(
                domain=domain, qmap_id=self.clean_name,
                ocl_env_url=ocl_env_url, ocl_api_token=ocl_api_token):
            does_source_exist = True

        # Generate the resources for import
        qmap_csv_resources = self.generate_import_script(
            domain=domain, do_create_org=do_create_org, do_create_source=do_create_source)
        qmap_json_resources = qmap_csv_resources.convert_to_ocl_formatted_json()

        # Display debug output
        if verbosity:
            print '\n**** Org and source in OCL:'
            print '  Org "%s" already exists: %s' % (domain, str(does_org_exist))
            print '  Source "%s" already exists: %s' % (self.clean_name, str(does_source_exist))
            if does_source_exist:
                print '  Existing source "%s" will be deleted and recreated...' % self.clean_name
            print '\n**** CSV Resources:'
            qmap_csv_resources.display_as_csv()
            print '\n**** Converted JSON Resources:'
            for resource in qmap_json_resources:
                print json.dumps(resource)

        # Validate
        qmap_csv_resources.validate()
        qmap_json_resources.validate()

        # Exit now if in test mode
        if test_mode:
            print "\nTEST MODE: Skipping import"
            return None

        # Delete source if it already exists
        # TODO: Shift to updating source in place in the future
        if does_source_exist:
            Qmap.delete_source(domain=domain, qmap_id=self.clean_name,
                               ocl_env_url=ocl_env_url, ocl_api_token=ocl_api_token)

        # Submit the bulk import
        import_response = ocldev.oclfleximporter.OclBulkImporter.post(
            input_list=qmap_json_resources, api_url_root=ocl_env_url,
            api_token=ocl_api_token, test_mode=test_mode)
        import_response.raise_for_status()
        import_response_json = import_response.json()
        task_id = import_response_json['task']
        return task_id

    @staticmethod
    def delete_source(domain='', qmap_id='', ocl_env_url='', ocl_api_token=''):
        qmap_repo_url = '%s%s' % (ocl_env_url, ocldev.oclconstants.OclConstants.get_repository_url(
            owner_id=domain, repository_id=qmap_id, include_trailing_slash=True))
        ocl_api_headers = {'Content-Type': 'application/json'}
        if ocl_api_token:
            ocl_api_headers['Authorization'] = 'Token ' + ocl_api_token
        response = requests.delete(qmap_repo_url, headers=ocl_api_headers)
        response.raise_for_status()
        if response.status_code == 204:
            # Successfully deleted
            return True
        raise Exception("Unexpected response code when deleting source '%s': %s" % (
            qmap_repo_url, response.status_code))

    def generate_import_script(self, domain='', do_create_org=True, do_create_source=True):
        """ Generate OCL-formatted CSV import script """

        # Load the questionnaire
        questionnaire_filename = 'qmap-questionnaires/%s.json' % self.questionnaireuid
        qmap_questionnaire = fhir.Questionnaire.load_from_file(questionnaire_filename)

        # Instantiate the resource list and add org and source resources
        qmap_csv_resources = ocldev.oclresourcelist.OclCsvResourceList()
        if do_create_org:
            qmap_csv_resources.append(self._get_csv_resource_organization(
                org_id=domain, name=domain, custom_attr={"qmap_org": True}))
        if do_create_source:
            qmap_csv_resources.append(self._get_csv_resource_source(
                source_id=self.clean_name,
                name=self.name,
                owner_id=domain,
                external_id=self.uid,
                custom_attr={'questionnaireuid': self.questionnaireuid,
                             'qmap_version': Qmap.QMAP_VERSION,
                             'questionnaire': json.dumps(qmap_questionnaire.to_json())}
            ))

        # Build the Qmap CSV concepts with their mappings for Qmap headers
        for qmap_key, qmap_item in self.headers.items():
            pos_header_concept = self._generate_pos_header_concept_and_mapping(
                qmap_key=qmap_key, qmap_item=qmap_item, owner=domain, source=self.clean_name,
                qmap_questionnaire=qmap_questionnaire,
                owner_type=ocldev.oclconstants.OclConstants.RESOURCE_TYPE_ORGANIZATION)
            qmap_csv_resources.append(pos_header_concept)
            if 'choiceMap' in qmap_item:
                pos_header_choices = self._generate_pos_choice_concepts_and_mappings(
                    qmap_key=qmap_key, qmap_item=qmap_item, owner=domain, source=self.clean_name,
                    qmap_questionnaire=qmap_questionnaire,
                    owner_type=ocldev.oclconstants.OclConstants.RESOURCE_TYPE_ORGANIZATION)
                qmap_csv_resources.append(pos_header_choices)

        # Build concepts/mappings for Qmap constants
        for qmap_key, qmap_item in self.constants.items():
            _pos_constant_concept = self._generate_pos_constant_concept_and_mapping(
                qmap_key=qmap_key, qmap_item=qmap_item, owner=domain, source=self.clean_name,
                qmap_questionnaire=qmap_questionnaire,
                owner_type=ocldev.oclconstants.OclConstants.RESOURCE_TYPE_ORGANIZATION)
            qmap_csv_resources.append(_pos_constant_concept)

        # Add a new source version
        qmap_csv_resources.append(self._get_csv_resource_repo_version(
            owner_id=domain, owner_type=ocldev.oclconstants.OclConstants.RESOURCE_TYPE_ORGANIZATION,
            repo_type=ocldev.oclconstants.OclConstants.RESOURCE_TYPE_SOURCE,
            repo_id=self.clean_name, version_id="v1.0", description="Auto-generated version"))

        return qmap_csv_resources

    @staticmethod
    def _clean_org_and_source_id(dirty_id):
        clean_id = dirty_id.replace('_', '-')
        return clean_id

    def _get_csv_resource_repo_version(self, owner_id='', owner_type='', repo_type='', repo_id='',
                                       version_id='', description='', released=True):
        """ Get OCL-formatted CSV Repository Version resource """
        csv_resource = {
            "resource_type": '%s Version' % repo_type,
            "owner_id": owner_id,
            "owner_type": owner_type,
            repo_type.lower(): repo_id,
            "id": version_id,
            "description": description,
            "released": released
        }
        return csv_resource

    def _get_csv_resource_organization(self, org_id='', name='',
                                       location='', company='', website='', custom_attr=None):
        """ Get OCL-formatted CSV Organization resource """
        csv_resource = {
            "resource_type": "Organization",
            "id": org_id,
            "name": name,
        }
        if location:
            csv_resource["location"] = location
        if company:
            csv_resource["company"] = company
        if website:
            csv_resource["website"] = website
        if custom_attr:
            for key in custom_attr:
                csv_resource['attr:%s' % key] = custom_attr[key]
        return csv_resource

    def _get_csv_resource_source(self, source_id='', name='', owner_id='', external_id='',
                                 custom_attr=None):
        """ Get OCL-formatted CSV Source resource """
        csv_resource = {
            "resource_type": "Source",
            "id": source_id,
            "name": name,
            "owner_id": owner_id,
            "external_id": external_id,
        }
        if custom_attr:
            for key in custom_attr:
                csv_resource['attr:%s' % key] = custom_attr[key]
        return csv_resource

    def _generate_pos_header_concept_and_mapping(self, qmap_key='', qmap_item=None,
                                                 source='', qmap_questionnaire=None, owner='',
                                                 owner_type=ocldev.oclconstants.OclConstants.RESOURCE_TYPE_ORGANIZATION):
        concept = {
            'resource_type': ocldev.oclconstants.OclConstants.RESOURCE_TYPE_CONCEPT,
            'id': Qmap._convert_to_pos_header_concept_id(qmap_key),
            'owner_id': owner,
            'owner_type': owner_type,
            'source': source,
            'name': qmap_key,
            'concept_class': self.QMAP_CONCEPT_CLASS_POS_HEADER,
            'datatype': qmap_item['valueType'],
            'attr:path': qmap_item['path'],
            'extmap_type[01]': 'Same As',
            'extmap_to_source_url[01]': ocldev.oclconstants.OclConstants.get_repository_url(
                owner_id=qmap_questionnaire.owner, repository_id=qmap_questionnaire.source),
            'extmap_to_concept_id[01]': Qmap._convert_to_questionnaire_concept_id(qmap_item),
        }
        return concept

    def _generate_pos_constant_concept_and_mapping(self, qmap_key='', qmap_item=None,
                                                   source='', qmap_questionnaire=None, owner='',
                                                   owner_type=ocldev.oclconstants.OclConstants.RESOURCE_TYPE_ORGANIZATION):
        pos_constant_concept = {
            'resource_type': ocldev.oclconstants.OclConstants.RESOURCE_TYPE_CONCEPT,
            'id': Qmap._convert_to_pos_header_concept_id(qmap_key),
            'owner_id': owner,
            'owner_type': owner_type,
            'source': source,
            'name': qmap_item["display"],
            'name_type': 'display',
            'name[2]': qmap_item["code"],
            'name_type[2]': 'code',
            'concept_class': self.QMAP_CONCEPT_CLASS_POS_CONSTANT,
            'datatype': qmap_item['valueType'],
            'attr:path': qmap_item['path'],
        }
        return pos_constant_concept

    def _generate_pos_choice_concepts_and_mappings(self, qmap_key='', qmap_item=None, source='',
                                                   qmap_questionnaire=None, owner='',
                                                   owner_type=ocldev.oclconstants.OclConstants.RESOURCE_TYPE_ORGANIZATION):
        choice_concepts = []
        base_row = {
            'resource_type': ocldev.oclconstants.OclConstants.RESOURCE_TYPE_CONCEPT,
            'owner_id': owner,
            'owner_type': owner_type,
            'source': source,
            'concept_class': self.QMAP_CONCEPT_CLASS_POS_CHOICE,
            'datatype': 'None',
            'attr:path': qmap_item['path'],
            'attr:header_concept_id': Qmap._convert_to_pos_header_concept_id(qmap_key),
            'extmap_type[01]': 'Same As',
        }
        for choice_pos_value, choice_questionnaire_value in qmap_item['choiceMap'].items():
            choice_concept = base_row.copy()
            choice_concept['id'] = Qmap._convert_to_pos_choice_concept_id(
                qmap_key, choice_pos_value)
            choice_concept['name'] = choice_pos_value
            choice_concept['attr:questionnaire_choice_value'] = choice_questionnaire_value
            choice_concept['extmap_to_source_url[01]'] = ocldev.oclconstants.OclConstants.get_repository_url(
                owner_id=qmap_questionnaire.owner, repository_id=qmap_questionnaire.source,
                include_trailing_slash=True)
            choice_concept['extmap_to_concept_id[01]'] = self._get_questionnaire_choice_concept_id(
                qmap_item, choice_questionnaire_value)
            choice_concepts.append(choice_concept)
        return choice_concepts

    @staticmethod
    def _convert_to_pos_header_concept_id(qmap_key):
        chars_to_remove = '/\"\'~`!@#$%^-&*()<>,:;{}[]|?'
        rx = '[' + re.escape(chars_to_remove) + ']'
        new_qmap_key = '-'.join(re.sub(rx, ' ', qmap_key).strip().split())
        return new_qmap_key

    @staticmethod
    def _convert_to_pos_choice_concept_id(qmap_key, choice_value):
        return '%s.%s' % (
            Qmap._convert_to_pos_header_concept_id(qmap_key),
            Qmap._convert_to_pos_header_concept_id(choice_value))

    @staticmethod
    def _convert_to_questionnaire_concept_id(qmap_item):
        key = qmap_item['path'][-1:][0]['linkid']
        start_index = 0
        if key[0] == '/':
            start_index = 1
        return key[start_index:].replace('/', '.')

    def _get_questionnaire_choice_concept_id(self, qmap_item, choice_questionnaire_value):
        return '%s.%s' % (Qmap._convert_to_questionnaire_concept_id(qmap_item),
                          choice_questionnaire_value)

    @property
    def name(self):
        return self._qmap['name']

    @property
    def clean_name(self):
        return Qmap._clean_org_and_source_id(self._qmap['name'])

    @property
    def created(self):
        return self._qmap['created']

    @property
    def updated(self):
        return self._qmap['updated']

    @property
    def uid(self):
        return self._qmap['uid']

    @property
    def questionnaireuid(self):
        return self._qmap['questionnaireuid']

    @property
    def complete(self):
        return self._qmap['complete']

    @property
    def headers(self):
        if 'map' in self._qmap and 'headers' in self._qmap['map']:
            return self._qmap['map']['headers']
        return {}

    @property
    def constants(self):
        if 'map' in self._qmap and 'constants' in self._qmap['map']:
            return self._qmap['map']['constants']
        return {}
