import sys
import requests
from datimconstants import DatimConstants
from datimbase import DatimBase
from datimshow import DatimShow
from deepdiff import DeepDiff
from pprint import pprint


class DatimSyncTest(DatimBase):
    def __init__(self, oclenv='', oclapitoken='', dhis2env='', dhis2uid='', dhis2pwd=''):
        DatimBase.__init__(self)
        self.oclenv = oclenv
        self.oclapitoken = oclapitoken
        self.dhis2env = dhis2env
        self.dhis2uid = dhis2uid
        self.dhis2pwd = dhis2pwd
        self.oclapiheaders = {
            'Authorization': 'Token ' + self.oclapitoken,
            'Content-Type': 'application/json'
        }

    def test_all(self):
        for resource_type in DatimConstants.SYNC_RESOURCE_TYPES:
            self.test_resource_type(resource_type)

    def test_resource_type(self, resource_type):
        if resource_type == DatimConstants.IMPORT_BATCH_SIMS:
            self.test_sims()
        elif resource_type == DatimConstants.IMPORT_BATCH_MECHANISMS:
            self.test_mechanisms()
        elif resource_type == DatimConstants.IMPORT_BATCH_TIERED_SUPPORT:
            self.test_tiered_support()
        elif resource_type == DatimConstants.IMPORT_BATCH_MER:
            self.test_mer()
        else:
            print('ERROR: Unrecognized resource_type "%s"' % resource_type)
            sys.exit(1)

    def test_sims(self):
        self.test_default(DatimConstants.SIMS_OCL_EXPORT_DEFS, 'datim-sims')

    def test_mechanisms(self):
        self.test_default(DatimConstants.MECHANISMS_OCL_EXPORT_DEFS, 'datim-mechanisms')

    def test_tiered_support(self):
        self.test_default(DatimConstants.TIERED_SUPPORT_OCL_EXPORT_DEFS, 'datim-tiered-support')

    def test_default(self, export_defs, openhim_endpoint):
        for export_def_key in export_defs:
            # Iterate through the formats
            # for format in DatimShow.PRESENTATION_FORMATS:
            for format in [DatimShow.DATIM_FORMAT_JSON]:
                if 'dhis2_sqlview_id' not in export_defs[export_def_key]:
                    continue

                # Build the dhis2 presentation url
                dhis2_presentation_url = self.replace_attr(
                    DatimConstants.DHIS2_PRESENTATION_URL_DEFAULT,
                    {'format': format, 'sqlview': export_defs[export_def_key]['dhis2_sqlview_id']})

                # Build the OCL presentation url
                openhimenv = 'https://ocl-mediator-trial.ohie.org:5000/'
                ocl_presentation_url = '%s%s?format=%s&collection=%s' % (
                    openhimenv, openhim_endpoint, format, export_def_key)

                self.test_one(format, dhis2_presentation_url, ocl_presentation_url)

    def test_mer(self):
        for export_def_key in DatimConstants.MER_OCL_EXPORT_DEFS:
            # Fetch the external_id from OCL, which is the DHIS2 dataSet uid
            url_ocl_repo = self.oclenv + DatimConstants.MER_OCL_EXPORT_DEFS[export_def_key]['endpoint']
            r = requests.get(url_ocl_repo, headers=self.oclapiheaders)
            repo = r.json()
            print('%s: %s' % (DatimConstants.MER_OCL_EXPORT_DEFS[export_def_key]['endpoint'], repo['external_id']))
            if not repo['external_id']:
                print '\tSkipping because no external ID...'
                continue

            # Iterate through the formats
            # for format in DatimShow.PRESENTATION_FORMATS:
            for format in [DatimShow.DATIM_FORMAT_JSON]:

                # Build the dhis2 presentation url
                dhis2_presentation_url = self.replace_attr(
                    DatimConstants.DHIS2_PRESENTATION_URL_MER,
                    {'format': format, 'dataset': repo['external_id']})

                # Build the OCL presentation url
                openhimenv = 'https://ocl-mediator-trial.ohie.org:5000/'
                ocl_presentation_url = '%sdatim-mer?format=%s&collection=%s' % (openhimenv, format, export_def_key)

                self.test_one(format, dhis2_presentation_url, ocl_presentation_url)

    def test_one(self, format, dhis2_presentation_url, ocl_presentation_url):
        print ('\tFormat = %s' % format)
        print('\t\tDHIS2: %s' % dhis2_presentation_url)
        print('\t\tOCL:   %s' % ocl_presentation_url)

        # Get the DHIS2 json
        r = requests.get(dhis2_presentation_url)
        dhis2_json = r.json()

        # Get the OCL json
        r = requests.get(ocl_presentation_url, verify=False)
        ocl_json = r.json()

        diff = DeepDiff(dhis2_json, ocl_json, ignore_order=True, verbose_level=2)
        pprint(diff)

# OCL Settings - JetStream Staging user=datim-admin
oclenv = 'https://api.staging.openconceptlab.org'
oclapitoken = 'c3b42623c04c87e266d12ae0e297abbce7f1cbe8'

datim_test = DatimSyncTest(oclenv=oclenv, oclapitoken=oclapitoken)
datim_test.test_all()
