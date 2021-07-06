"""
Class to import into OCL a country indicator mapping file (IMAP) as CSV or JSON for a
specified country (e.g. UG) and period (e.g. FY19). IMAP must follow the format of
the country indicator mapping template.

The import script creates OCL-formatted JSON consisting of:
    Country Org (e.g. DATIM-MOH-UG)
    Country Source (e.g. DATIM-Alignment-Indicators)
    One concept for each country unique indicator/data element and unique disag
    One mapping for each unique country indicator+disag pair with an operation map type (e.g. ADD, SUBTRACT)
    One mapping for each PEPFAR indicator+disag pair represented with a "DATIM HAS OPTION" map type
    Country Collections, one per mapping to DATIM indicator+disag pair
    References for each concept and mapping added to each collection
"""
import json
import datimbase
import datimimap
import ocldev.oclfleximporter
import ocldev.oclexport
import ocldev.oclconstants
import ocldev.oclresourcelist
import utils.timer


class ImapCountryLockedForPeriodError(Exception):
    """ ImapCountryLockedForPeriodError """
    pass


class DatimImapImport(datimbase.DatimBase):
    """
    Class to import DATIM country indicator mapping metadata into OCL.
    """

    DATIM_IMAP_RESULT_SUCCESS = 1
    DATIM_IMAP_RESULT_WARNING = 0
    DATIM_IMAP_RESULT_ERROR = -1

    def __init__(self, oclenv='', oclapitoken='', verbosity=0, run_ocl_offline=False,
                 test_mode=False, country_public_access='View'):
        datimbase.DatimBase.__init__(self)
        self.verbosity = verbosity
        self.oclenv = oclenv
        self.oclapitoken = oclapitoken
        self.run_ocl_offline = run_ocl_offline
        self.test_mode = test_mode
        self.country_public_access = country_public_access

        # Prepare the headers
        self.oclapiheaders = {
            'Authorization': 'Token ' + self.oclapitoken,
            'Content-Type': 'application/json'
        }

    def import_imap(self, imap_input=None):
        """
        Import the specified IMAP into OCL
        :param imap_input: IMAP to import
        :return: OCL bulk import status ID if successfully submitted. None if nothing to import.
        """

        # Validate input variables
        if not self.oclapitoken or not self.oclapiheaders:
            msg = 'ERROR: Authorization token must be set'
            self.vlog(1, msg)
            raise Exception(msg)

        # STEP 1 of 5: Make sure an import for same country+period is not underway
        imap_timer = utils.timer.Timer()
        imap_timer.start()
        self.vlog(1, '**** STEP 1 of 5: Make sure an import for same country+period is not underway')
        status_filter = ['PENDING', 'STARTED']
        queued_imports = ocldev.oclfleximporter.OclBulkImporter.get_queued_imports(
            api_url_root=self.oclenv, api_token=self.oclapitoken, queue=imap_input.country_org,
            status_filter=status_filter)
        if queued_imports:
            err_msg = 'IMAP import is already underway for same country and period: %s' % (
                imap_input.country_org)
            raise ImapCountryLockedForPeriodError(err_msg)
        imap_timer.lap(label='STEP 1: Make sure an import for same country+period is not underway')

        # STEP 2 of 5: Download PEPFAR/DATIM-MOH-FY## export for specified period from OCL
        self.vlog(1, '**** STEP 2 of 5: Download PEPFAR/DATIM-MOH-FY## export for specified period')
        self.datim_moh_source_id = datimbase.DatimBase.get_datim_moh_source_id(imap_input.period)
        datim_source_endpoint = datimbase.DatimBase.get_datim_moh_source_endpoint(imap_input.period)
        datim_source_version = self.get_latest_version_for_period(
            repo_endpoint=datim_source_endpoint, period=imap_input.period)
        if not datim_source_version:
            msg = 'ERROR: Could not find released version for period "%s" for source "%s"' % (
                imap_input.period, datim_source_endpoint)
            self.vlog(1, msg)
            raise Exception(msg)
        self.vlog(1, 'Latest version found for period "%s" for source "%s": "%s"' % (
            imap_input.period, datim_source_endpoint, datim_source_version))
        repo_version_url = '%s%s/%s/' % (
                self.oclenv, datim_source_endpoint, datim_source_version)
        repo_version_url = '%s%s' % (self.oclenv, datim_source_endpoint)
        if datim_source_endpoint[-1] != "/":
            repo_version_url += "/"
        repo_version_url += '%s/' % (datim_source_version)
        datim_moh_source_export = ocldev.oclexport.OclExportFactory.load_export(
            repo_version_url=repo_version_url, oclapitoken=self.oclapitoken)
        imap_timer.lap(label='STEP 3: Download DATIM-MOH-FYxx Export')

        # STEP 3 of 5: Validate input country mapping CSV file
        # NOTE: This currently just verifies that the correct columns exist (order agnostic)
        # TODO: Validate that DATIM indicator/disag IDs in the provided IMAP are valid
        self.vlog(1, '**** STEP 3 of 5: Validate country IMAP input file')
        if self.verbosity:
            imap_input.display(exclude_empty_maps=True, auto_fix_null_disag=True)
        is_valid = imap_input.is_valid(datim_moh_source_export=datim_moh_source_export)
        if type(is_valid) == str:
            self.vlog(1, 'WARNING: The following warnings were found in the provided IMAP:\n',
                      is_valid)
        else:
            self.vlog(1, 'The provided IMAP passed validation')
        imap_timer.lap(label='STEP 3: Validate country IMAP input file')

        # STEP 4 of 5: Generate IMAP import script
        self.vlog(1, '**** STEP 4 of 5: Generate IMAP import script')
        import_list = ocldev.oclresourcelist.OclJsonResourceList()
        does_imap_org_exist = datimimap.DatimImapFactory.check_if_imap_org(
            org_id=imap_input.country_org, ocl_env_url=self.oclenv,
            ocl_api_token=self.oclapitoken, verbose=bool(self.verbosity))
        if does_imap_org_exist:
            self.vlog(1, 'Org "%s" already exists.' % imap_input.country_org)
            import_list.append({
                '__action': 'DELETE',
                'type': 'Organization',
                'id': imap_input.country_org
            })
        else:
            self.vlog(1, 'Org "%s" not found.' % imap_input.country_org)
        import_list.append(datimimap.DatimImapFactory.generate_resource_list_from_imap(
            imap_input=imap_input, verbose=bool(self.verbosity)))
        if self.verbosity >= 2:
            for resource in import_list:
                print json.dumps(resource)
        imap_timer.lap(label='STEP 4: Generate IMAP import script')

        # STEP 5 of 5: Bulk import into OCL
        # NOTE: Everything is non-destructive up to this point. Changes are committed to OCL here.
        self.vlog(1, '**** STEP 5 of 5: Bulk import into OCL')
        if import_list and not self.test_mode:
            self.vlog(1, 'Bulk importing %s resources to OCL...' % len(import_list))
            bulk_import_response = ocldev.oclfleximporter.OclBulkImporter.post(
                input_list=import_list, api_token=self.oclapitoken, api_url_root=self.oclenv,
                queue=imap_input.country_org, parallel=True)
            bulk_import_response.raise_for_status()
            task_id = bulk_import_response.json()['task']
            self.vlog(1, 'BULK IMPORT TASK ID: %s' % task_id)
            imap_timer.stop(label='STOP')
            self.vlog(1, '** IMAP import time breakdown:\n', imap_timer)
            return task_id
        elif self.test_mode:
            self.vlog(1, 'TEST MODE: Skipping import...')
            imap_timer.stop(label='STOP')
            self.vlog(1, '** IMAP import time breakdown:\n', imap_timer)
        else:
            self.vlog(1, 'SKIPPING: Empty IMAP. Nothing to import!')
            imap_timer.stop(label='STOP')
            self.vlog(1, '** IMAP import time breakdown:\n', imap_timer)
        return None
