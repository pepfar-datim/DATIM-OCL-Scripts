"""
OCL Flexible Importer --
Script that uses the OCL API to import multiple resource types from a JSON lines file.
Configuration for individual resources can be set inline in the JSON.

Resources currently supported:
* Organizations
* Sources
* Collections
* Concepts
* Mappings
* References

Verbosity settings:
* 0 = show only responses from server
* 1 = show responses from server and all POSTs
* 2 = show everything minus debug output
* 3 = show everything plus debug output

Deviations from OCL API responses:
* Sources/Collections:
    - "supported_locales" response is a list in OCL, but only a comma-separated string
      is supported when posted here
"""

import json
import requests
import sys
from datetime import datetime
import urllib


# Owner fields: ( owner AND owner_type ) OR ( owner_url )
# Repository fields: ( source OR source_url ) OR ( collection OR collection_url )
# Concept/Mapping fieldS: ( id ) OR ( url )


class OclImportError(Exception):
    """ Base exception for this module """
    pass


class UnexpectedStatusCodeError(OclImportError):
    """ Exception raised for unexpected status code """
    def __init__(self, expression, message):
        self.expression = expression
        self.message = message


class InvalidOwnerError(OclImportError):
    """ Exception raised when owner information is invalid """
    def __init__(self, expression, message):
        self.expression = expression
        self.message = message


class InvalidRepositoryError(OclImportError):
    """ Exception raised when repository information is invalid """
    def __init__(self, expression, message):
        self.expression = expression
        self.message = message


class InvalidObjectDefinition(OclImportError):
    """ Exception raised when object definition invalid """
    def __init__(self, expression, message):
        self.expression = expression
        self.message = message


class OclFlexImporter:
    """ Class to flexibly import multiple resource types into OCL from JSON lines files via the OCL API """

    INTERNAL_MAPPING = 1
    EXTERNAL_MAPPING = 2

    OBJ_TYPE_USER = 'User'
    OBJ_TYPE_ORGANIZATION = 'Organization'
    OBJ_TYPE_SOURCE = 'Source'
    OBJ_TYPE_COLLECTION = 'Collection'
    OBJ_TYPE_CONCEPT = 'Concept'
    OBJ_TYPE_MAPPING = 'Mapping'
    OBJ_TYPE_REFERENCE = 'Reference'

    ACTION_TYPE_NEW = 'new'
    ACTION_TYPE_UPDATE = 'udpate'
    ACTION_TYPE_RETIRE = 'retire'
    ACTION_TYPE_DELETE = 'delete'
    ACTION_TYPE_OTHER = 'other'

    # Resource type definitions
    obj_def = {
        OBJ_TYPE_ORGANIZATION: {
            "id_field": "id",
            "url_name": "orgs",
            "has_owner": False,
            "has_source": False,
            "has_collection": False,
            "allowed_fields": ["id", "company", "extras", "location", "name", "public_access", "extras", "website"],
            "create_method": "POST",
            "update_method": "POST",
        },
        OBJ_TYPE_SOURCE: {
            "id_field": "id",
            "url_name": "sources",
            "has_owner": True,
            "has_source": False,
            "has_collection": False,
            "allowed_fields": ["id", "short_code", "name", "full_name", "description", "source_type", "custom_validation_schema", "public_access", "default_locale", "supported_locales", "website", "extras", "external_id"],
            "create_method": "POST",
            "update_method": "POST",
        },
        OBJ_TYPE_COLLECTION: {
            "id_field": "id",
            "url_name": "collections",
            "has_owner": True,
            "has_source": False,
            "has_collection": False,
            "allowed_fields": ["id", "short_code", "name", "full_name", "description", "collection_type", "custom_validation_schema", "public_access", "default_locale", "supported_locales", "website", "extras", "external_id"],
            "create_method": "POST",
            "update_method": "POST",
        },
        OBJ_TYPE_CONCEPT: {
            "id_field": "id",
            "url_name": "concepts",
            "has_owner": True,
            "has_source": True,
            "has_collection": False,
            "allowed_fields": ["id", "external_id", "concept_class", "datatype", "names", "descriptions", "retired", "extras"],
            "create_method": "POST",
            "update_method": "POST",
        },
        OBJ_TYPE_MAPPING: {
            "id_field": "id",
            "url_name": "mappings",
            "has_owner": True,
            "has_source": True,
            "has_collection": False,
            "allowed_fields": ["id", "map_type", "from_concept_url", "to_source_url", "to_concept_url", "to_concept_code", "to_concept_name", "extras", "external_id"],
            "create_method": "POST",
            "update_method": "POST",
        },
        OBJ_TYPE_REFERENCE: {
            "url_name": "references",
            "has_owner": True,
            "has_source": False,
            "has_collection": True,
            "allowed_fields": ["data"],
            "create_method": "PUT",
            "update_method": None,
        },
    }

    def __init__(self, file_path='', api_url_root='', api_token='', limit=0,
                 test_mode=False, verbosity=1, do_update_if_exists=False):
        """ Initialize this object """

        self.file_path = file_path
        self.api_token = api_token
        self.api_url_root = api_url_root
        self.test_mode = test_mode
        self.do_update_if_exists = do_update_if_exists
        self.verbosity = verbosity
        self.limit = limit

        self.results = {}
        self.cache_obj_exists = {}

        # Prepare the headers
        self.api_headers = {
            'Authorization': 'Token ' + self.api_token,
            'Content-Type': 'application/json'
        }

    def log(self, *args):
        """ Output log information """
        sys.stdout.write('[' + str(datetime.now()) + '] ')
        for arg in args:
            sys.stdout.write(str(arg))
            sys.stdout.write(' ')
        sys.stdout.write('\n')
        sys.stdout.flush()

    def log_settings(self):
        """ Output log of the object settings """
        self.log("**** OCL IMPORT SCRIPT SETTINGS ****",
                 "API Root URL:", self.api_url_root,
                 ", API Token:", self.api_token,
                 ", Import File:", self.file_path,
                 ", Test Mode:", self.test_mode,
                 ", Update Resource if Exists: ", self.do_update_if_exists,
                 ", Verbosity:", self.verbosity)

    def process(self):
        """
        Imports a JSON-lines file using OCL API
        :return: int Number of JSON lines processed
        """

        # Display global settings
        if self.verbosity:
            self.log_settings()

        # Loop through each JSON object in the file
        obj_def_keys = self.obj_def.keys()
        with open(self.file_path) as json_file:
            count = 0
            for json_line_raw in json_file:
                if self.limit > 0 and count >= self.limit:
                    break
                json_line_obj = json.loads(json_line_raw)
                if "type" in json_line_obj:
                    obj_type = json_line_obj.pop("type")
                    if obj_type in obj_def_keys:
                        self.process_object(obj_type, json_line_obj)
                    else:
                        self.log("**** SKIPPING: Unrecognized 'type' attribute '" + obj_type + "' for object: " + json_line_raw)
                else:
                    self.log("**** SKIPPING: No 'type' attribute: " + json_line_raw)
                count += 1

        return count

    def does_object_exist(self, obj_url, use_cache=True):
        """ Returns whether an object at the specified URL already exists """

        # If obj existence cached, then just return True
        if use_cache and obj_url in self.cache_obj_exists and self.cache_obj_exists[obj_url]:
            return True

        # Object existence not cached, so use API to check if it exists
        request_existence = requests.head(self.api_url_root + obj_url, headers=self.api_headers)
        if request_existence.status_code == requests.codes.ok:
            self.cache_obj_exists[obj_url] = True
            return True
        elif request_existence.status_code == requests.codes.not_found:
            return False
        else:
            raise UnexpectedStatusCodeError(
                "GET " + self.api_url_root + obj_url,
                "Unexpected status code returned: " + str(request_existence.status_code))

    def does_mapping_exist(self, obj_url, obj):
        """
        Returns whether the specified mapping already exists --
        Equivalent mapping must have matching source, from_concept, to_concept, and map_type
        """

        '''
        # Return false if correct fields not set
        mapping_target = None
        if ('from_concept_url' not in obj or not obj['from_concept_url']
                or 'map_type' not in obj or not obj['map_type']):
            # Invalid mapping -- no from_concept or map_type
            return False
        if 'to_concept_url' in obj:
            mapping_target = self.INTERNAL_MAPPING
        elif 'to_source_url' in obj and 'to_concept_code' in obj:
            mapping_target = self.EXTERNAL_MAPPING
        else:
            # Invalid to_concept
            return False

        # Build query parameters
        params = {
            'fromConceptOwner': '',
            'fromConceptOwnerType': '',
            'fromConceptSource': '',
            'fromConcept': obj['from_concept_url'],
            'mapType': obj['map_type'],
            'toConceptOwner': '',
            'toConceptOwnerType': '',
            'toConceptSource': '',
            'toConcept': '',
        }
        #if mapping_target == self.INTERNAL_MAPPING:
        #    params['toConcept'] = obj['to_concept_url']
        #else:
        #    params['toConcept'] = obj['to_concept_code']

        # Use API to check if mapping exists
        request_existence = requests.head(
            self.api_url_root + obj_url, headers=self.api_headers, params=params)
        if request_existence.status_code == requests.codes.ok:
            if 'num_found' in request_existence.headers and int(request_existence.headers['num_found']) >= 1:
                return True
            else:
                return False
        elif request_existence.status_code == requests.codes.not_found:
            return False
        else:
            raise UnexpectedStatusCodeError(
                "GET " + self.api_url_root + obj_url,
                "Unexpected status code returned: " + str(request_existence.status_code))
        '''

        return False

    def does_reference_exist(self, obj_url, obj):
        """ Returns whether the specified reference already exists """

        '''
        # Return false if no expression
        if 'expression' not in obj or not obj['expression']:
            return False

        # Use the API to check if object exists
        params = {'q': obj['expression']}
        request_existence = requests.head(
            self.api_url_root + obj_url, headers=self.api_headers, params=params)
        if request_existence.status_code == requests.codes.ok:
            if 'num_found' in request_existence.headers and int(request_existence.headers['num_found']) >= 1:
                return True
            else:
                return False
        elif request_existence.status_code == requests.codes.not_found:
            return False
        else:
            raise UnexpectedStatusCodeError(
                "GET " + self.api_url_root + obj_url,
                "Unexpected status code returned: " + str(request_existence.status_code))
        '''

        return False

    def process_object(self, obj_type, obj):
        """ Processes an individual document in the import file """

        # Grab the ID
        obj_id = ''
        if 'id_field' in self.obj_def[obj_type] and self.obj_def[obj_type]['id_field'] in obj:
            obj_id = obj[self.obj_def[obj_type]['id_field']]

        # Set owner URL using ("owner_url") OR ("owner" AND "owner_type")
        # e.g. /users/johndoe/ OR /orgs/MyOrganization/
        # NOTE: Owner URL always ends with a forward slash
        has_owner = False
        obj_owner_url = None
        if self.obj_def[obj_type]["has_owner"]:
            has_owner = True
            if "owner_url" in obj:
                obj_owner_url = obj.pop("owner_url")
                obj.pop("owner", None)
                obj.pop("owner_type", None)
            elif "owner" in obj and "owner_type" in obj:
                obj_owner_type = obj.pop("owner_type")
                obj_owner = obj.pop("owner")
                if obj_owner_type == self.OBJ_TYPE_ORGANIZATION:
                    obj_owner_url = "/" + self.obj_def[self.OBJ_TYPE_ORGANIZATION]["url_name"] + "/" + obj_owner + "/"
                elif obj_owner_url == self.OBJ_TYPE_USER:
                    obj_owner_url = "/" + self.obj_def[self.OBJ_TYPE_USER]["url_name"] + "/" + obj_owner + "/"
                else:
                    raise InvalidOwnerError(obj, "Valid owner information required for object of type '" + obj_type + "'")
            else:
                raise InvalidOwnerError(obj, "Valid owner information required for object of type '" + obj_type + "'")

        # Set repository URL using ("source_url" OR "source") OR ("collection_url" OR "collection")
        # e.g. /orgs/MyOrganization/sources/MySource/ OR /orgs/CIEL/collections/StarterSet/
        # NOTE: Repository URL always ends with a forward slash
        has_source = False
        has_collection = False
        obj_repo_url = None
        if self.obj_def[obj_type]["has_source"] and self.obj_def[obj_type]["has_collection"]:
            raise InvalidObjectDefinition(obj, "Object definition for '" + obj_type + "' must not have both 'has_source' and 'has_collection' set to True")
        elif self.obj_def[obj_type]["has_source"]:
            has_source = True
            if "source_url" in obj:
                obj_repo_url = obj.pop("source_url")
                obj.pop("source", None)
            elif "source" in obj:
                obj_repo_url = obj_owner_url + 'sources/' + obj.pop("source") + "/"
            else:
                raise InvalidRepositoryError(obj, "Valid source information required for object of type '" + obj_type + "'")
        elif self.obj_def[obj_type]["has_collection"]:
            has_collection = True
            if "collection_url" in obj:
                obj_repo_url = obj.pop("collection_url")
                obj.pop("collection", None)
            elif "collection" in obj:
                obj_repo_url = obj_owner_url + 'collections/' + obj.pop("collection") + "/"
            else:
                raise InvalidRepositoryError(obj, "Valid collection information required for object of type '" + obj_type + "'")

        # Build object URLs -- note that these always end with forward slashes
        if has_source or has_collection:
            if obj_id:
                # Concept
                new_obj_url = obj_repo_url + self.obj_def[obj_type]["url_name"] + "/"
                obj_url = new_obj_url + obj_id + "/"
            else:
                # Mapping, reference, etc.
                new_obj_url = obj_url = obj_repo_url + self.obj_def[obj_type]["url_name"] + "/"
        elif has_owner:
            # Repositories (source or collection) and anything that also has a repository
            new_obj_url = obj_owner_url + self.obj_def[obj_type]["url_name"] + "/"
            obj_url = new_obj_url + obj_id + "/"
        else:
            # Only organizations and users don't have an owner or repository -- and only orgs can be created here
            new_obj_url = '/' + self.obj_def[obj_type]["url_name"] + "/"
            obj_url = new_obj_url + obj_id + "/"

        # Handle query parameters
        # NOTE: This is hard coded just for references for now
        query_params = {}
        if obj_type == self.OBJ_TYPE_REFERENCE:
            if "__cascade" in obj:
                query_params["cascade"] = obj.pop("__cascade")

        # Pull out the fields that aren't allowed
        obj_not_allowed = {}
        for k in obj.keys():
            if k not in self.obj_def[obj_type]["allowed_fields"]:
                obj_not_allowed[k] = obj.pop(k)

        # Display some debug info
        if self.verbosity >= 1:
            self.log("**** Importing " + obj_type + ": " + self.api_url_root + obj_url + " ****")
        if self.verbosity >= 2:
            self.log("** Allowed Fields: **", json.dumps(obj))
            self.log("** Removed Fields: **", json.dumps(obj_not_allowed))

        # Check if owner exists
        if has_owner and obj_owner_url:
            try:
                if self.does_object_exist(obj_owner_url):
                    self.log("** INFO: Owner exists at: " + obj_owner_url)
                else:
                    self.log("** SKIPPING: Owner does not exist at: " + obj_owner_url)
                    if not self.test_mode:
                        return
            except UnexpectedStatusCodeError as e:
                self.log("** SKIPPING: Unexpected error occurred: ", e.expression, e.message)
                return

        # Check if repository exists
        if (has_source or has_collection) and obj_repo_url:
            try:
                if self.does_object_exist(obj_repo_url):
                    self.log("** INFO: Repository exists at: " + obj_repo_url)
                else:
                    self.log("** SKIPPING: Repository does not exist at: " + obj_repo_url)
                    if not self.test_mode:
                        return
            except UnexpectedStatusCodeError as e:
                self.log("** SKIPPING: Unexpected error occurred: ", e.expression, e.message)
                return

        # Check if object already exists: GET self.api_url_root + obj_url
        obj_already_exists = False
        try:
            if obj_type == 'Reference':
                obj_already_exists = self.does_reference_exist(obj_url, obj)
            elif obj_type == 'Mapping':
                obj_already_exists = self.does_mapping_exist(obj_url, obj)
            else:
                obj_already_exists = self.does_object_exist(obj_url)
        except UnexpectedStatusCodeError as e:
            self.log("** SKIPPING: Unexpected error occurred: ", e.expression, e.message)
            return
        if obj_already_exists and not self.do_update_if_exists:
            self.log("** SKIPPING: Object already exists at: " + self.api_url_root + obj_url)
            if not self.test_mode:
                return
        elif obj_already_exists:
            self.log("** INFO: Object already exists at: " + self.api_url_root + obj_url)
        else:
            self.log("** INFO: Object does not exist so we'll create it at: " + self.api_url_root + obj_url)

        # TODO: Validate the JSON object

        # Create/update the object
        try:
            self.update_or_create(
                obj_type=obj_type,
                obj_id=obj_id,
                obj_owner_url=obj_owner_url,
                obj_repo_url=obj_repo_url,
                obj_url=obj_url,
                new_obj_url=new_obj_url,
                obj_already_exists=obj_already_exists,
                obj=obj, obj_not_allowed=obj_not_allowed,
                query_params=query_params)
        except requests.exceptions.HTTPError as e:
            self.log("ERROR: ", e)

    def update_or_create(self, obj_type='', obj_id='', obj_owner_url='',
                         obj_repo_url='', obj_url='', new_obj_url='',
                         obj_already_exists=False,
                         obj=None, obj_not_allowed=None,
                         query_params=None):
        """ Posts an object to the OCL API as either an update or create """

        # Determine which URL to use based on whether or not object already exists
        action_type = None
        if obj_already_exists:
            method = self.obj_def[obj_type]['update_method']
            url = obj_url
            action_type = self.ACTION_TYPE_UPDATE
        else:
            method = self.obj_def[obj_type]['create_method']
            url = new_obj_url
            action_type = self.ACTION_TYPE_NEW

        # Add query parameters (if provided)
        if query_params:
            url += '?' + urllib.urlencode(query_params)

        # Get out of here if in test mode
        if self.test_mode:
            self.log("[TEST MODE] ", method, self.api_url_root + url + '  ', json.dumps(obj))
            return

        # Determine method
        if obj_already_exists:
            # Skip updates for now
            self.log("[SKIPPING UPDATE] ", method, self.api_url_root + url + '  ', json.dumps(obj))
            return

        # Create or update the object
        self.log(method, " ", self.api_url_root + url + '  ', json.dumps(obj))
        if method == 'POST':
            request_result = requests.post(self.api_url_root + url, headers=self.api_headers,
                                         data=json.dumps(obj))
        elif method == 'PUT':
            request_result = requests.put(self.api_url_root + url, headers=self.api_headers,
                                         data=json.dumps(obj))
        self.log("STATUS CODE:", request_result.status_code)
        self.log(request_result.headers)
        self.log(request_result.text)
        request_result.raise_for_status()

        # Store the results if successful
        # TODO: This could be improved significantly!
        if int(request_result.status_code) > 200 and int(request_result.status_code) < 300:
            if obj_type in [self.OBJ_TYPE_CONCEPT, self.OBJ_TYPE_MAPPING, self.OBJ_TYPE_REFERENCE]:
                if obj_repo_url not in self.results:
                    self.results[obj_repo_url] = {}
                if action_type not in self.results[obj_repo_url]:
                    self.results[obj_repo_url][action_type] = []
                self.results[obj_repo_url][action_type].append(obj_url)
            elif obj_type in [self.OBJ_TYPE_SOURCE, self.OBJ_TYPE_COLLECTION]:
                if obj_owner_url not in self.results:
                    self.results[obj_owner_url] = {}
                if action_type not in self.results[obj_owner_url]:
                    self.results[obj_owner_url][action_type] = []
                self.results[obj_owner_url][action_type].append(obj_url)
            elif obj_type == [self.OBJ_TYPE_ORGANIZATION]:
                if '/orgs/' not in self.results:
                    self.results['/orgs/'] = {}
                if action_type not in self.results['/orgs/']:
                    self.results['/orgs/'][action_type] = []
                self.results['/orgs/'][action_type].append(obj_url)
            elif obj_type == [self.OBJ_TYPE_USER]:
                if '/users/' not in self.results:
                    self.results['/users/'] = {}
                if action_type not in self.results['/users/']:
                    self.results['/users/'][action_type] = []
                self.results['/users/'][action_type].append(obj_url)
