"""
Script to present DATIM MOH metadata

Supported Formats: html, xml, csv, json
OpenHIM Mediator Request Format: /datim-moh?period=____&format=____
"""

from . import datimconstants, datimshow


class DatimShowMoh(datimshow.DatimShow):
    """ Class to manage DATIM MOH Presentation """

    # OCL Export Definitions
    OCL_EXPORT_DEFS = datimconstants.DatimConstants.MOH_OCL_EXPORT_DEFS

    REQUIRE_OCL_EXPORT_DEFINITION = True

    # Output headers
    headers = {
        'moh': [
            {"name": "dataset", "column": "dataset", "type": "java.lang.String", "hidden": False,
             "meta": False},
            {"name": "dataelement", "column": "dataelement", "type": "java.lang.String",
             "hidden": False, "meta": False},
            {"name": "shortname", "column": "shortname", "type": "java.lang.String",
             "hidden": False, "meta": False},
            {"name": "code", "column": "code", "type": "java.lang.String", "hidden": False,
             "meta": False},
            {"name": "dataelementuid", "column": "dataelementuid", "type": "java.lang.String",
             "hidden": False, "meta": False},
            {"name": "dataelementdesc", "column": "dataelementdesc","type": "java.lang.String",
             "hidden": False, "meta": False},
            {"name": "categoryoptioncombo", "column": "categoryoptioncombo",
             "type": "java.lang.String", "hidden": False, "meta": False},
            {"name": "categoryoptioncombocode", "column": "categoryoptioncombocode",
             "type": "java.lang.String", "hidden": False, "meta": False},
            {"name": "categoryoptioncombouid", "column": "categoryoptioncombouid",
             "type": "java.lang.String", "hidden": False, "meta": False},
            {"name": "classification", "column": "classification", "type": "java.lang.String",
             "hidden": False, "meta": False},
        ]
    }

    def __init__(self, oclenv='', oclapitoken='', run_ocl_offline=False, verbosity=0,
                 cache_intermediate=True):
        """ Initialize DatimShowMoh object """
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

    def get(self, period='', export_format=''):
        """
        Overrides underlying method simply to change the parameter name to period and to
        add validation
        """
        return datimshow.DatimShow.get(self, repo_id=period, export_format=export_format)

    def build_moh_indicator_output(self, c, headers=None, direct_mappings=None, repo_title='',
                                   repo_subtitle=''):
        """ Return one or more output rows for the specified input concept """

        if c['concept_class'] != 'Data Element':
            return None

        # Build output row template, pre-populating with the current data element attributes
        concept_description = ''
        if c['descriptions']:
            concept_description = c['descriptions'][0]['description']
        output_concept_template = {
            'dataset': repo_title,
            'dataelement': c['names'][0]['name'],
            'shortname': c['names'][1]['name'],
            'code': c['id'],
            'dataelementuid': c['external_id'],
            'dataelementdesc': concept_description,
            'categoryoptioncombo': '',
            'categoryoptioncombocode': '',
            'categoryoptioncombouid': '',
            'classification': '',
        }

        # Find all the relevant mappings
        if direct_mappings:
            output_rows = []
            for m in direct_mappings:
                output_concept = output_concept_template.copy()
                output_concept['categoryoptioncombo'] = m['to_concept_name']
                if m['to_concept_code'] == self.DATIM_DEFAULT_DISAG_ID:
                    output_concept['categoryoptioncombocode'] = m['to_concept_name']
                else:
                    output_concept['categoryoptioncombocode'] = m['to_concept_code']
                output_concept['categoryoptioncombouid'] = m['to_concept_code']
                if ('to_concept' in m and 'extras' in m['to_concept'] and
                        'classification' in m['to_concept']['extras']):
                    output_concept['classification'] = m['to_concept']['extras']['classification']
                output_rows.append(output_concept)
            return output_rows
        else:
            return output_concept_template
