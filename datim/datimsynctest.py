"""
Class to test the synchronization by comparing the resulting metadata presentation
formats from DHIS2 and OCL.
"""
import sys
import requests
import warnings
import difflib
from pprint import pprint
from deepdiff import DeepDiff
from operator import itemgetter
import datimconstants
import datimbase
import datimshow

class DatimSyncTest(datimbase.DatimBase):
    def __init__(self, oclenv='', oclapitoken='', formats=None, dhis2env='', dhis2uid='', dhis2pwd=''):
        datimbase.DatimBase.__init__(self)
        self.oclenv = oclenv
        self.oclapitoken = oclapitoken
        self.dhis2env = dhis2env
        self.dhis2uid = dhis2uid
        self.dhis2pwd = dhis2pwd
        if formats:
            self.formats = formats
        else:
            self.formats = datimshow.DatimShow.PRESENTATION_FORMATS
        self.oclapiheaders = {
            'Authorization': 'Token ' + self.oclapitoken,
            'Content-Type': 'application/json'
        }

    def test_all(self):
        for resource_type in datimconstants.DatimConstants.SYNC_RESOURCE_TYPES:
            self.test_resource_type(resource_type)

    def test_resource_type(self, resource_type):
        if resource_type == datimconstants.DatimConstants.IMPORT_BATCH_SIMS:
            self.test_sims()
        elif resource_type == datimconstants.DatimConstants.IMPORT_BATCH_MECHANISMS:
            self.test_mechanisms()
        elif resource_type == datimconstants.DatimConstants.IMPORT_BATCH_TIERED_SUPPORT:
            self.test_tiered_support()
        elif resource_type == datimconstants.DatimConstants.IMPORT_BATCH_MER:
            self.test_mer()
        else:
            print('ERROR: Unrecognized resource_type "%s"' % resource_type)
            sys.exit(1)

    def test_sims(self):
        self.test_default(datimconstants.DatimConstants.SIMS_OCL_EXPORT_DEFS,
                          datimconstants.DatimConstants.OPENHIM_ENDPOINT_SIMS)

    def test_mechanisms(self):
        self.test_default(datimconstants.DatimConstants.MECHANISMS_OCL_EXPORT_DEFS,
                          datimconstants.DatimConstants.OPENHIM_ENDPOINT_MECHANISMS)

    def test_tiered_support(self):
        self.test_default(datimconstants.DatimConstants.TIERED_SUPPORT_OCL_EXPORT_DEFS,
                          datimconstants.DatimConstants.OPENHIM_ENDPOINT_TIERED_SUPPORT)

    def test_default(self, export_defs, openhim_endpoint):
        for export_def_key in export_defs:
            # Iterate through the formats
            for format in self.formats:
                if 'dhis2_sqlview_id' not in export_defs[export_def_key]:
                    continue

                # Build the dhis2 presentation url
                dhis2_presentation_url = self.replace_attr(
                    datimconstants.DatimConstants.DHIS2_PRESENTATION_URL_DEFAULT,
                    {'format': format, 'sqlview': export_defs[export_def_key]['dhis2_sqlview_id']})

                # Build the OCL presentation url
                openhimenv = 'https://ocl-mediator-trial.ohie.org:5000/'
                ocl_presentation_url = '%s%s?format=%s&collection=%s' % (
                    openhimenv, openhim_endpoint, format, export_def_key)

                self.test_one(format, dhis2_presentation_url, ocl_presentation_url)

    def test_mer(self):
        for export_def_key in datimconstants.DatimConstants.MER_OCL_EXPORT_DEFS:
            # Fetch the external_id from OCL, which is the DHIS2 dataSet uid
            url_ocl_repo = self.oclenv + datimconstants.DatimConstants.MER_OCL_EXPORT_DEFS[export_def_key]['endpoint']
            r = requests.get(url_ocl_repo, headers=self.oclapiheaders)
            repo = r.json()
            print('\n**** %s (dataSet.id=%s) ****' % (datimconstants.DatimConstants.MER_OCL_EXPORT_DEFS[export_def_key]['endpoint'], repo['external_id']))
            if not repo['external_id']:
                print('Skipping because no external ID...')
                continue

            # Iterate through the formats
            for format in self.formats:

                # Build the dhis2 presentation url
                dhis2_presentation_url = self.replace_attr(
                    datimconstants.DatimConstants.DHIS2_PRESENTATION_URL_MER,
                    {'format': format, 'dataset': repo['external_id']})

                # Build the OCL presentation url
                openhimenv = 'https://ocl-mediator-trial.ohie.org:5000/'
                ocl_presentation_url = '%s%s?format=%s&collection=%s' % (
                    openhimenv, datimconstants.DatimConstants.OPENHIM_ENDPOINT_MER, format, export_def_key)

                self.test_one(format, dhis2_presentation_url, ocl_presentation_url)

    def test_one(self, format, dhis2_presentation_url, ocl_presentation_url):
        print ('Format = %s' % format)
        print('DHIS2: %s' % dhis2_presentation_url)
        print('OCL:   %s' % ocl_presentation_url)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            request_dhis2 = requests.get(dhis2_presentation_url)
            request_ocl = requests.get(ocl_presentation_url, verify=False)
        diff = None
        if format == datimshow.DatimShow.DATIM_FORMAT_JSON:
            diff = self.test_json(request_dhis2, request_ocl)
        elif format == datimshow.DatimShow.DATIM_FORMAT_HTML:
            diff = self.test_html(request_dhis2, request_ocl)
        elif format == datimshow.DatimShow.DATIM_FORMAT_XML:
            diff = self.test_xml(request_dhis2, request_ocl)
        elif format == datimshow.DatimShow.DATIM_FORMAT_CSV:
            diff = self.test_csv(request_dhis2, request_ocl)
        if diff:
            print('Diff Results:')
            pprint(diff)
        else:
            print('No diff!')
        sys.stdout.flush()

    def test_json(self, request_dhis2, request_ocl):
        # Prepare the diff
        dhis2_json = request_dhis2.json()
        ocl_json = request_ocl.json()
        dhis2_json['rows_dict'] = {}
        ocl_json['rows_dict'] = {}
        for row in dhis2_json['rows']:
            dhis2_json['rows_dict']['%s-%s' % (row[4], row[7])] = row[1:]
        for row in ocl_json['rows']:
            ocl_json['rows_dict']['%s-%s' % (row[4], row[7])] = row[1:]
        del(dhis2_json['rows'])
        del(ocl_json['rows'])
        print('Rows: DHIS2(%s), OCL(%s)' % (len(dhis2_json['rows_dict']), len(ocl_json['rows_dict'])))

        # Do the diff
        diff = DeepDiff(dhis2_json, ocl_json, ignore_order=False, verbose_level=2, exclude_paths={"root['title']", "root['subtitle']"})
        return diff

    def test_html(self, request_dhis2, request_ocl):
        d = difflib.Differ()
        return d.compare(request_dhis2.text.splitlines(1), request_ocl.text.splitlines(1))

    def test_xml(self, request_dhis2, request_ocl):
        d = difflib.Differ()
        return d.compare(request_dhis2.text.splitlines(1), request_ocl.text.splitlines(1))

    def test_csv(self, request_dhis2, request_ocl):
        d = difflib.Differ()
        return d.compare(request_dhis2.text.splitlines(1), request_ocl.text.splitlines(1))
