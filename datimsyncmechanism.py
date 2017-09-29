"""
Class to synchronize DATIM-DHIS2 Mechanisms definitions with OCL
The script runs 1 import batch, which consists of two queries to DHIS2, which are
synchronized with repositories in OCL as described below.
|-------------|-------------------------|--------------------------------------------|
| ImportBatch | DHIS2                   | OCL                                        |
|-------------|-------------------------|--------------------------------------------|
| Mechanisms        | MechanismsAssessmentTypeQuery | /orgs/PEPFAR/sources/Mechanisms/                 |
|             |                         | /orgs/PEPFAR/collections/Mechanisms3-Facility/   |
|             |                         | /orgs/PEPFAR/collections/Mechanisms3-Community/  |
|             |                         | /orgs/PEPFAR/collections/Mechanisms3-Above-Site/ |
|             |                         | /orgs/PEPFAR/collections/Mechanisms2-Facility/   |
|             |                         | /orgs/PEPFAR/collections/Mechanisms2-Community/  |
|             |                         | /orgs/PEPFAR/collections/Mechanisms2-Above-Site/ |
|             |-------------------------|--------------------------------------------|
|             | MechanismsOptionsQuery        | /orgs/PEPFAR/sources/Mechanisms/                 |
|             |                         | /orgs/PEPFAR/collections/Mechanisms-Options/     |
|-------------|-------------------------|--------------------------------------------|
"""
from __future__ import with_statement
import os
import sys
import json
from datimsync import DatimSync


class DatimSyncMechanisms(DatimSync):
    """ Class to manage DATIM Mechanisms Synchronization """

    # Dataset ID settings
    OCL_DATASET_ENDPOINT = '/orgs/PEPFAR/collections/?q=Mechanisms&verbose=true&limit=200'
    REPO_ACTIVE_ATTR = 'datim_sync_Mechanisms'
    DATASET_REPOSITORIES_FILENAME = 'ocl_dataset_repos_export.json'

    # Filenames
    NEW_IMPORT_SCRIPT_FILENAME = 'Mechanisms_dhis2ocl_import_script.json'
    DHIS2_CONVERTED_EXPORT_FILENAME = 'Mechanisms_dhis2_converted_export.json'
    OCL_CLEANED_EXPORT_FILENAME = 'Mechanisms_ocl_cleaned_export.json'

    # Import batches
    IMPORT_BATCH_Mechanisms = 'Mechanisms'
    IMPORT_BATCHES = [IMPORT_BATCH_Mechanisms]

    # DATIM DHIS2 Query Definitions
    DHIS2_QUERIES = {
        'Mechanisms': {
            'id': 'Mechanisms',
            'name': 'DATIM-DHIS2 Mechanisms',
            'query': 'api/categoryOptionCombos.json?fields=id,code,name,created,lastUpdated,
                     'categoryOptions[id,endDate,startDate,organisationUnits[code,name],
                     'categoryOptionGroups[id,name,code,groupSets[id,name]]]&order=code:asc&filter=categoryCombo.id:eq:wUpfppgjEza&paging=false',
            'conversion_method': 'dhis2diff_Mechanisms'
        }
    }
    DHIS2_QUERIES_INACTIVE = {
        'MechanismsOptions': {
            'id': 'MechanismsOptions',
            'name': 'DATIM-DHIS2 Mechanisms Options',
            'query': '',
            'conversion_method': 'dhis2diff_Mechanisms_options'
        }
    }

    # OCL Export Definitions
    OCL_EXPORT_DEFS = {
        'Mechanisms_source': {'import_batch': IMPORT_BATCH_Mechanisms,
                        'endpoint': '/orgs/PEPFAR/sources/Mechanisms/'}
    }

    def __init__(self, oclenv='', oclapitoken='', dhis2env='', dhis2uid='', dhis2pwd='', compare2previousexport=True,
                 runoffline=False, verbosity=0, data_check_only=False, import_test_mode=False, import_limit=0):
        DatimSync.__init__(self)

        self.oclenv = oclenv
        self.oclapitoken = oclapitoken
        self.dhis2env = dhis2env
        self.dhis2uid = dhis2uid
        self.dhis2pwd = dhis2pwd
        self.runoffline = runoffline
        self.verbosity = verbosity
        self.compare2previousexport = compare2previousexport
        self.import_test_mode = import_test_mode
        self.import_limit = import_limit
        self.data_check_only = data_check_only
        self.oclapiheaders = {
            'Authorization': 'Token ' + self.oclapitoken,
            'Content-Type': 'application/json'
        }

    def dhis2diff_Mechanisms_options(self, dhis2_query_def=None, conversion_attr=None):
        """
        Convert new DHIS2 Mechanisms Options export to the diff format
        :param dhis2_query_def:
        :param conversion_attr:
        :return:
        """
        pass

    def dhis2diff_Mechanisms(self, dhis2_query_def=None, conversion_attr=None):
        """
        Convert new DHIS2 Mechanisms export to the diff format
        :param dhis2_query_def: DHIS2 query definition
        :param conversion_attr: Optional dictionary of attributes to pass to the conversion method
        :return: Boolean
        """
        dhis2filename_export_new = self.dhis2filename_export_new(dhis2_query_def['id'])
        with open(self.attach_absolute_path(dhis2filename_export_new), "rb") as input_file:
            new_dhis2_export = json.load(input_file)
            ocl_dataset_repos = conversion_attr['ocl_dataset_repos']
            num_concepts = 0
            gs_iteration_count = 0
            output = []
            orgunit = ''
            c = {}

            # Iterate through each DataElement and transform to an OCL-JSON concept
            # UPDATED - This section is specific to the metadata (Indicator, Dissagregation, SIMS, Mechanism etc.)
            for coc in new_dhis2_export['categoryOptionCombos']:
                concept_id = coc['code']
                #print coc['name']
                for co in coc['categoryOptions']:
                    costartDate = co.get('startDate', '')
                    coendDate = co.get('endDate', '')
                    for ou in co["organisationUnits"]:
                        print "inside OU"
                        orgunit = ou.get('name', '');
                    for cog in co['categoryOptionGroups']:
                        cogid = cog['id'];
                        cogname = cog['name'];
                        cogcode = cog.get('code', '');
                        for gs in cog['groupSets']:
                            print 'Length %s' % (len(gs))
                            print 'Iteration Count %s' % (gs_iteration_count)
                            groupsetname = gs['name'];
                            print groupsetname
                            if groupsetname == 'Funding Agency':
                                agency = cogname
                                print agency
                            elif groupsetname == 'Implementing Partner':
                                partner = cogname
                                primeid = cogcode
                            if gs_iteration_count == len(gs):
                                print "inside IF"
                                c = {
                                    'type':'Concept',
                                    'concept_id':concept_id,
                                    'concept_class':'Funding Mechanism',
                                    'datatype':'Text',
                                    'owner':'PEPFAR',
                                    'owner_type':'Organization',
                                    'source':'Mechanisms',
                                    'external_id':coc['id'],
                                    'names':[
                                        {'name':coc['name'], 'name_type':'Fully Specified', 'locale':'en'}
                                    ],
                                     'extras':{'Partner':partner,
                                               'Prime Id':primeid,
                                               'Agency':agency,
                                               'Start Date':costartDate,
                                               'End Date':coendDate,
                                               'Organizational Unit':orgunit}
                                }
                            gs_iteration_count += 1

                self.dhis2_diff[self.IMPORT_BATCH_Mechanisms][self.RESOURCE_TYPE_CONCEPT][concept_key] = c
                num_concepts += 1
                gs_iteration_count = 0

                #UPDATED Removed section that was previously iterating through each DataElementGroup and transform to an OCL-JSON reference

            #ofile.write(']')
            ofile.write(json.dumps(output))

        self.vlog(1, 'DHIS2 export "%s" successfully transformed to %s concepts + %s references (%s total)' % (
            dhis2filename_export_new, num_concepts, num_references, num_concepts + num_references))
        return True


# Default Script Settings
verbosity = 2  # 0=none, 1=some, 2=all
import_limit = 0  # Number of resources to import; 0=all
import_test_mode = False  # Set to True to see which import API requests would be performed on OCL
runoffline = False  # Set to true to use local copies of dhis2/ocl exports
compare2previousexport = True  # Set to False to ignore the previous export

# DATIM DHIS2 Settings
dhis2env = ''
dhis2uid = ''
dhis2pwd = ''

# OCL Settings
oclenv = ''
oclapitoken = ''

# Set variables from environment if available
if len(sys.argv) > 1 and sys.argv[1] in ['true', 'True']:
    # Server environment settings (required for OpenHIM)
    dhis2env = os.environ['DHIS2_ENV']
    dhis2uid = os.environ['DHIS2_USER']
    dhis2pwd = os.environ['DHIS2_PASS']
    oclenv = os.environ['OCL_ENV']
    oclapitoken = os.environ['OCL_API_TOKEN']
    compare2previousexport = os.environ['COMPARE_PREVIOUS_EXPORT'] in ['true', 'True']
else:
    # Local development environment settings
    import_limit = 1
    import_test_mode = True
    compare2previousexport = False
    runoffline = False
    dhis2env = 'https://dev-de.datim.org/'
    dhis2uid = 'jpayne'
    dhis2pwd = 'Johnpayne1!'

    # Digital Ocean Showcase - user=paynejd99
    # oclenv = 'https://api.showcase.openconceptlab.org'
    # oclapitoken = '2da0f46b7d29aa57970c0b3a535121e8e479f881'

    # JetStream Staging - user=paynejd
    # oclenv = 'https://oclapi-stg.openmrs.org'
    # oclapitoken = 'a61ba53ed7b8b26ece8fcfc53022b645de0ec055'

    # JetStream QA - user=paynejd
    oclenv = 'https://oclapi-qa.openmrs.org'
    oclapitoken = 'a5678e5f7971f3003e7be563ee4b90297b841f05'


# Create sync object and run
Mechanisms_sync = DatimSyncMechanisms(oclenv=oclenv, oclapitoken=oclapitoken,
                          dhis2env=dhis2env, dhis2uid=dhis2uid, dhis2pwd=dhis2pwd,
                          compare2previousexport=compare2previousexport,
                          runoffline=runoffline, verbosity=verbosity,
                          import_test_mode=import_test_mode,
                          import_limit=import_limit)
# Mechanisms_sync.run()
Mechanisms_sync.data_check()
