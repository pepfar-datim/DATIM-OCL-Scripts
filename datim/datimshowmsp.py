"""
Script to present MSP Metadata in the DATIM codelist format.
Supports display of an entire codelist or a list of data element IDs.
See showmsp.py for an example of usage.

Supported Formats: html, xml, csv, json
"""
from __future__ import with_statement
import requests
import datimshow
import datimbase


class DatimShowMsp(datimshow.DatimShow):
    """ Class to manage DATIM MSP Presentation """

    # OCL Export Definitions
    # OCL_EXPORT_DEFS = datimconstants.DatimConstants.MER_OCL_EXPORT_DEFS
    OCL_EXPORT_DEFS = {}

    REQUIRE_OCL_EXPORT_DEFINITION = False

    # Default endpoint to use if unspecified OCL export
    DEFAULT_REPO_LIST_ENDPOINT = '/orgs/PEPFAR-Test6/collections/'

    DEFAULT_SHOW_BUILD_ROW_METHOD = 'build_mer_data_element_output'

    # Output headers
    headers = {
        'msp': [
            {"name": "dataset", "column": "dataset", "type": "java.lang.String",
             "hidden": False, "meta": False},
            {"name": "dataelement", "column": "dataelement", "type": "java.lang.String",
             "hidden": False, "meta": False},
            {"name": "shortname", "column": "shortname", "type": "java.lang.String",
             "hidden": False, "meta": False},
            {"name": "code", "column": "code", "type": "java.lang.String",
             "hidden": False, "meta": False},
            {"name": "dataelementuid", "column": "dataelementuid", "type": "java.lang.String",
             "hidden": False, "meta": False},
            {"name": "dataelementdesc", "column": "dataelementdesc", "type": "java.lang.String",
             "hidden": False, "meta": False},
            {"name": "categoryoptioncombo", "column": "categoryoptioncombo",
             "type": "java.lang.String", "hidden": False, "meta": False},
            {"name": "categoryoptioncombocode", "column": "categoryoptioncombocode",
             "type": "java.lang.String", "hidden": False, "meta": False},
            {"name": "categoryoptioncombouid", "column": "categoryoptioncombouid",
             "type": "java.lang.String", "hidden": False, "meta": False}
        ]
    }

    def __init__(self, oclenv='', oclapitoken='', run_ocl_offline=False, verbosity=0,
                 cache_intermediate=True):
        datimshow.DatimShow.__init__(self)
        self.oclenv = oclenv
        self.oclapitoken = oclapitoken
        self.run_ocl_offline = run_ocl_offline
        self.verbosity = verbosity
        self.cache_intermediate = cache_intermediate
        self.oclapiheaders = {
            'Content-Type': 'application/json'
        }
        if self.oclapitoken:
            self.oclapiheaders['Authorization'] = 'Token ' + self.oclapitoken

    def get(self, data_element_ids=None, repo_id='', export_format='',
            owner='PEPFAR', owner_type='Organization', source='MER'):
        """
        Overrides DatimShow.get to display an export of either a list of Data Element IDs
        or an entire Code List (repo_id).
        """
        if data_element_ids:
            self.vlog(1, 'Loading specified data element IDs: %s' % str(data_element_ids))
            if isinstance(data_element_ids, str):
                data_element_ids = data_element_ids.split(',')
            if not isinstance(data_element_ids, list):
                raise Exception('Invalid list of data element IDs: %s' % str(data_element_ids))
            data_element_results = []
            for data_element_id in data_element_ids:
                data_element_url = '%s/%s/%s/sources/%s/concepts/%s/?includeMappings=true' % (
                    self.oclenv, datimbase.DatimBase.owner_type_to_stem(owner_type),
                    owner, source, str(data_element_id.strip()))
                self.vlog(2, data_element_url)
                data_element_response = requests.get(data_element_url, headers=self.oclapiheaders)
                if data_element_response.status_code == 404:
                    self.vlog(1, '404 NOT FOUND')
                    continue
                # For other errors, go ahead and raise an exception
                data_element_response.raise_for_status()
                data_element_results.append(data_element_response.json())
            intermediate = self.build_show_grid(
                repo_title='Custom Query',
                repo_subtitle='',
                headers=self.headers['msp'],
                concepts_with_mappings=data_element_results,
                show_build_row_method='build_mer_data_element_output')
            self.transform_to_format(intermediate, export_format)
        elif repo_id:
            datimshow.DatimShow.get(self, repo_id=repo_id, export_format=export_format)
        else:
            raise Exception('Must provide either data_element_ids or repo_id.')

    def build_mer_data_element_output(self, concept, headers=None, direct_mappings=None,
                                      repo_title='', repo_subtitle=''):
        """ Builds export rows  """
        if concept['concept_class'] != 'Data Element':
            return None

        # Build the indicator
        output_concept = {
            'dataset': repo_title,
            'dataelement': concept['names'][0]['name'],
            'shortname': '',
            'code': concept['id'],
            'dataelementuid': concept.get('external_id', ''),
            'dataelementdesc': '',
            'categoryoptioncombo': '',
            'categoryoptioncombocode': '',
            'categoryoptioncombouid': '',
        }
        if len(concept['names']) > 1:
            output_concept['shortname'] = concept['names'][1]['name']
        if concept['descriptions']:
            output_concept['dataelementdesc'] = concept['descriptions'][0]['description']

        # Simply return the concept if there are no mappings
        if not direct_mappings:
            return output_concept

        # Return list of one concept per direct mapping
        output_rows = []
        for mapping in direct_mappings:
            output_concept['categoryoptioncombo'] = mapping['to_concept_name']
            output_concept['categoryoptioncombocode'] = mapping['to_concept_code']
            output_concept['categoryoptioncombouid'] = mapping['to_concept_code']
            output_rows.append(output_concept.copy())
        return output_rows
