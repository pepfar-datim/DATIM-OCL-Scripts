from __future__ import with_statement
import os
import sys
import json
from datimshow import DatimShow
from datimbase import DatimConstants


class DatimShowMechanisms(DatimShow):
    """ Class to manage DATIM Mechanisms Presentation """

    # OCL Export Definitions
    OCL_EXPORT_DEFS = DatimConstants.MECHANISMS_OCL_EXPORT_DEFS

    def __init__(self, oclenv='', oclapitoken='',
                 run_ocl_offline=False, verbosity=0):
        self.oclenv = oclenv
        self.oclapitoken = oclapitoken
        self.run_ocl_offline = run_ocl_offline
        self.verbosity = verbosity
        self.oclapiheaders = {
            'Authorization': 'Token ' + self.oclapitoken,
            'Content-Type': 'application/json'
        }

    def get(self, repo_id='Mechanisms', export_format=None):
        """
        Output the specified collection the requested format
        :param export_format: Format to present the data in
        :return: None
        """

        # Validate export_format
        if export_format not in self.PRESENTATION_FORMATS:
            export_format = self.DATIM_FORMAT_HTML

        # Get the export definition
        repo_title = ''
        repo_subtitle = ''
        if repo_id in self.OCL_EXPORT_DEFS:
            repo_endpoint = self.OCL_EXPORT_DEFS[repo_id]['endpoint']
            if 'title' in self.OCL_EXPORT_DEFS[repo_id]:
                repo_title = self.OCL_EXPORT_DEFS[repo_id]['title']
            if 'subtitle' in self.OCL_EXPORT_DEFS[repo_id]:
                repo_subtitle = self.OCL_EXPORT_DEFS[repo_id]['subtitle']
        else:
            repo_endpoint = '/orgs/PEPFAR/collections/%s/' % repo_id
        if not repo_title:
            repo_title = repo_id

        # STEP 1. Fetch latest version of relevant OCL repository export
        self.vlog(1, '**** STEP 1: Fetch latest version of relevant OCL repository export')
        self.vlog(1, '%s:' % repo_endpoint)
        tarfilename = self.endpoint2filename_ocl_export_tar(repo_endpoint)
        jsonfilename = self.endpoint2filename_ocl_export_json(repo_endpoint)
        if not self.run_ocl_offline:
            self.get_ocl_export(endpoint=repo_endpoint, version='latest', tarfilename=tarfilename,
                                jsonfilename=jsonfilename)
        else:
            self.vlog(1, 'OCL-OFFLINE: Using local file "%s"...' % jsonfilename)
            if os.path.isfile(self.attach_absolute_path(jsonfilename)):
                self.vlog(1, 'OCL-OFFLINE: File "%s" found containing %s bytes. Continuing...' % (
                    jsonfilename, os.path.getsize(self.attach_absolute_path(jsonfilename))))
            else:
                self.log('ERROR: Could not find offline OCL file "%s". Exiting...' % jsonfilename)
                sys.exit(1)

        # STEP 2: Transform OCL export to intermediary state
        self.vlog(1, '**** STEP 2: Transform to intermediary state')
        intermediate = {}
        jsonfilename = self.endpoint2filename_ocl_export_json(repo_endpoint)
        intermediate_json_filename = self.endpoint2filename_ocl_export_intermediate_json(repo_endpoint)
        with open(self.attach_absolute_path(jsonfilename), 'rb') as ifile, open(
                self.attach_absolute_path(intermediate_json_filename), 'wb') as ofile:
            ocl_export_raw = json.load(ifile)
            intermediate = {
                'title': repo_title,
                'subtitle': repo_subtitle,
                'height': 0,
                'headers': [
                    {"name": "mechanism", "column": "mechanism", "type": "java.lang.String", "hidden": False, "meta": False},
                    {"name": "code", "column": "code", "type": "java.lang.String", "hidden": False, "meta": False},
                    {"name": "uid", "column": "uid", "type": "java.lang.String", "hidden": False, "meta": False},
                    {"name": "partner", "column": "partner", "type": "java.lang.String", "hidden": False, "meta": False},
                    {"name": "primeid", "column": "primeid", "type": "java.lang.String", "hidden": False, "meta": False},
                    {"name": "agency", "column": "agency","type": "java.lang.String", "hidden": False, "meta": False},
                    {"name": "ou", "column": "ou", "type": "java.lang.String", "hidden": False, "meta": False},
                    {"name": "startdate", "column": "startdate", "type": "java.lang.String", "hidden": False, "meta": False},
                    {"name": "enddate", "column": "enddate", "type": "java.lang.String", "hidden": False, "meta": False}
                ],
                'rows': []
            }
            intermediate['width'] = len(intermediate['headers'])

            # Iterate through concepts, clean, then write
            for c in ocl_export_raw['concepts']:
                if c['concept_class'] == 'Funding Mechanism':
                    # Build the indicator
                    concept_description = ''
                    if c['descriptions']:
                        concept_description = c['descriptions'][0]['description']
                    output_concept = {
                        'mechanism': c['names'][0]['name'],
                        'code': c['id'],
                        'uid': c['external_id'],
                        'partner': c['extras']['Partner'],
                        'primeid': c['extras']['Prime Id'],
                        'agency': c['extras']['Agency'],
                        'ou': c['extras']['Organizational Unit'],
                        'startdate': c['extras']['Start Date'],
                        'enddate': c['extras']['End Date'],
                    }
                    intermediate['rows'].append(output_concept.copy())
            intermediate['height'] = len(intermediate['rows'])

            # Write intermediate state as a file (for future caching)
            ofile.write(json.dumps(intermediate))
            self.vlog(1, 'Processed OCL export saved to "%s"' % intermediate_json_filename)

        # STEP 3: Transform to requested format and stream
        self.vlog(1, '**** STEP 3: Transform to requested format and stream')
        self.transform_to_format(intermediate, export_format)


# Default Script Settings
verbosity = 0  # 0=none, 1=some, 2=all
run_ocl_offline = True  # Set to true to use local copies of dhis2/ocl exports

# Set some defaults
export_format = DatimShow.DATIM_FORMAT_JSON

# Set arguments from the command line
if sys.argv and len(sys.argv) > 2:
    # Export Format - see constants in DatimShow class
    if sys.argv[1] in ['html', 'HTML']:
        export_format = DatimShow.DATIM_FORMAT_HTML
    if sys.argv[1] in ['xml', 'XML']:
        export_format = DatimShow.DATIM_FORMAT_XML
    if sys.argv[1] in ['json', 'JSON']:
        export_format = DatimShow.DATIM_FORMAT_JSON
    if sys.argv[1] in ['csv', 'CSV']:
        export_format = DatimShow.DATIM_FORMAT_CSV

# OCL Settings
#oclenv = ''
#oclapitoken = ''

# JetStream Staging user=datim-admin
oclenv = 'https://api.staging.openconceptlab.org'
oclapitoken = 'c3b42623c04c87e266d12ae0e297abbce7f1cbe8'

# Create Show object and run
datim_show = DatimShowMechanisms(oclenv=oclenv, oclapitoken=oclapitoken, run_ocl_offline=run_ocl_offline,
                                 verbosity=verbosity)
datim_show.get(export_format=export_format)
