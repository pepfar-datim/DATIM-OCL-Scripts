"""
Script to test IMAP imports and exports for FY18 and FY19

Supported Test Actions: IMPORT, EXPORT, COMPARE, DROP
Supported Test Assertions:
"""
import requests
import datim.datimimap
import datim.datimimapexport
import datim.datimimapimport
import utils.timer


class DatimImapTests:
    """ Class to test IMAP imports and exports """

    DATIM_OCL_TEST_TYPE_IMPORT = 'IMPORT'
    DATIM_OCL_TEST_TYPE_EXPORT = 'EXPORT'
    DATIM_OCL_TEST_TYPE_COMPARE = 'COMPARE'
    DATIM_OCL_TEST_TYPE_DROP_ORG = 'DROP'
    DATIM_OCL_TEST_TYPES = [
        DATIM_OCL_TEST_TYPE_IMPORT,
        DATIM_OCL_TEST_TYPE_EXPORT,
        DATIM_OCL_TEST_TYPE_COMPARE,
        DATIM_OCL_TEST_TYPE_DROP_ORG,
    ]

    IMAP_COMPARE_TYPE_FILE = 'file'
    IMAP_COMPARE_TYPE_OCL = 'ocl'
    IMAP_COMPARE_TYPE_RESULT = 'result'
    IMAP_COMPARE_TYPE_VARIABLE = 'variable'

    def __init__(self):
        self.results = {}
        self.__current_test_num = None
        self.__total_test_count = None
        self.__tests = None
        self.__timer = None

    def get_test_summary(self, test_args):
        summary = '\n\n%s\n' % ('*' * 100)
        if self.__current_test_num and self.__total_test_count:
            summary += '[Test %s of %s] ' % (self.__current_test_num, self.__total_test_count)
        summary += '%s("%s"): %s\n' % (
            test_args.get("test_type"), test_args.get("test_id"), test_args.get("test_description"))
        is_first_val = True
        for key, val in list(test_args.items()):
            if key in ["test_id", "test_type", "test_description"]:
                continue
            if not is_first_val:
                summary += ', '
            summary += '%s=%s' % (key, val)
            is_first_val = False
        summary += '\n' + '*' * 100
        return summary

    def get_export(self, args):
        datim_imap_export = datim.datimimapexport.DatimImapExport(
            oclenv=args['ocl_api_env'],
            oclapitoken=args['ocl_api_token'],
            verbosity=args.get('verbosity', 2),
            run_ocl_offline=args.get('run_ocl_offline', False)
        )
        try:
            imap = datim_imap_export.get_imap(
                period=args.get('period', ''),
                version=args.get('version', ''),
                country_org=args.get('country_org', ''),
                country_code=args.get('country_code', '')
            )
        except requests.exceptions.HTTPError as e:
            raise e
        imap.display(
            fmt=args.get('export_format'),
            sort=args.get('sort', True),
            exclude_empty_maps=args.get('exclude_empty_maps', True),
            include_extra_info=args.get('include_extra_info', False)
        )
        return imap

    def process_import(self, args):
        # First load the import CSV
        imap_input = datim.datimimap.DatimImapFactory.load_imap_from_csv(
            csv_filename=args.get('imap_import_filename'),
            period=args.get('period', ''),
            country_org=args.get('country_org'),
            country_name=args.get('country_name'),
            country_code=args.get('country_code')
        )
        if imap_input:
            print(('INFO: IMAP import file "%s" loaded successfully' % args.get('imap_import_filename', "")))
        elif not imap_input:
            errmsg = 'ERROR: Unable to load IMAP import file "%s"' % args.get('imap_import_filename', "")
            raise Exception(errmsg)

        # Now process the import
        imap_import = datim.datimimapimport.DatimImapImport(
            oclenv=args.get('ocl_api_env'),
            oclapitoken=args.get('ocl_api_token'),
            verbosity=args.get('verbosity', 2),
            run_ocl_offline=args.get('run_ocl_offline', False),
            test_mode=args.get('test_mode', False),
            country_public_access=args.get('country_public_access', 'View')
        )
        return imap_import.import_imap(imap_input=imap_input)

    def imap_compare(self, args):
        imap_a = self.get_imap(
            args['imap_a_type'],
            imap_result_id=args.get('imap_a_result_id'),
            imap_filename=args.get('imap_a_filename'),
            imap_object=args.get('imap_a_object'),
            period=args.get('imap_a_period'),
            version=args.get('imap_a_version'),
            country_org=args.get('imap_a_country_org'),
            country_code=args.get('imap_a_country_code'),
            country_name=args.get('imap_a_country_name'),
            oclenv=args.get('imap_a_ocl_api_env'),
            oclapitoken=args.get('imap_a_ocl_api_token'))
        if isinstance(imap_a, datim.datimimap.DatimImap):
            print('** IMAP-A loaded successfully:')
            imap_a.display(sort=True, exclude_empty_maps=True)
            print('\n')
        imap_b = self.get_imap(
            args['imap_b_type'],
            imap_result_id=args.get('imap_b_result_id'),
            imap_filename=args.get('imap_b_filename'),
            imap_object=args.get('imap_b_object'),
            period=args.get('imap_b_period'),
            version=args.get('imap_b_version'),
            country_org=args.get('imap_b_country_org'),
            country_code=args.get('imap_b_country_code'),
            country_name=args.get('imap_b_country_name'),
            oclenv=args.get('imap_b_ocl_api_env'),
            oclapitoken=args.get('imap_b_ocl_api_token'))
        if isinstance(imap_b, datim.datimimap.DatimImap):
            print('** IMAP-B loaded successfully:')
            imap_b.display(sort=True, exclude_empty_maps=True)
            print('\n')
        if isinstance(imap_a, datim.datimimap.DatimImap) and isinstance(imap_b, datim.datimimap.DatimImap):
            imap_diff = imap_a.diff(imap_b)
            imap_diff.display()
            return imap_diff
        return None

    def get_imap(self, imap_type, imap_result_id='', imap_filename='', imap_object=None,
                 period='', version='', country_org='', country_code='', country_name='',
                 oclenv='', oclapitoken=''):
        if imap_type == DatimImapTests.IMAP_COMPARE_TYPE_RESULT:
            if imap_result_id in self.results:
                if isinstance(self.results[imap_result_id], datim.datimimap.DatimImap):
                    return self.results[imap_result_id]
                else:
                    raise Exception('ERROR: Result ID "%s" not a valid DatimImap: %s' % (
                        imap_result_id, self.results[imap_result_id]))
            else:
                raise Exception('ERROR: No result ID "%s"' % imap_result_id)
        elif imap_type == DatimImapTests.IMAP_COMPARE_TYPE_FILE:
            return datim.datimimap.DatimImapFactory.load_imap_from_file(
                imap_filename=imap_filename, period=period,
                country_org=country_org, country_name=country_name, country_code=country_code)
        elif imap_type == DatimImapTests.IMAP_COMPARE_TYPE_OCL:
            datim_imap_export = datim.datimimapexport.DatimImapExport(
                oclenv=oclenv, oclapitoken=oclapitoken, verbosity=1, run_ocl_offline=False)
            return datim_imap_export.get_imap(
                period=period, version=version, country_org=country_org, country_code=country_code)
        elif imap_type == DatimImapTests.IMAP_COMPARE_TYPE_VARIABLE:
            return imap_object
        else:
            raise Exception('Unrecognized imap_type "%s". Must be one of the DatimImapTests.IMAP_COMPARE_TYPE_RESULT contants' % imap_type)

    def drop_org(self, args):
        return datim.datimimap.DatimImapFactory.delete_org_if_exists(
            org_id=args.get('country_org'),
            oclenv=args.get('ocl_api_env'),
            ocl_root_api_token=args.get('ocl_api_token'),
            verbose=True
        )

    def run_test_export(self, test_args):
        return self.get_export(test_args)

    def run_test_import(self, test_args):
        return self.process_import(test_args)

    def run_test_compare(self, test_args):
        return self.imap_compare(test_args)

    def run_test_drop_org(self, test_args):
        return self.drop_org(test_args)

    @staticmethod
    def assert_result_type(result, result_type):
        assert_result = isinstance(result, result_type)
        print("Assert Result Type: %s == %s -- %s" % (type(result), result_type, assert_result))

    @staticmethod
    def assert_result_value(result, result_value):
        assert_result = result == result_value
        print("Assert Result Value: %s == %s -- %s" % (result, result_value, assert_result))

    @staticmethod
    def assert_num_diff(result, target_num_diff):
        assert_result = False
        result_num_diff = 'N/A'
        if isinstance(result, datim.datimimap.DatimImapDiff):
            result_num_diff = result.get_num_diffs()
            assert_result = target_num_diff == result_num_diff
        print("Assert Num Diff: %s == %s -- %s" % (result_num_diff, target_num_diff, assert_result))

    @staticmethod
    def assert_http_response_code(result, http_response_code):
        result_response_code = None
        if isinstance(result, requests.exceptions.HTTPError) and hasattr(result, 'response'):
            result_response_code = result.response.status_code
        elif isinstance(result, requests.models.Response):
            result_response_code = result.response.status_code
        elif isinstance(result, int):
            result_response_code = result
        assert_result = result_response_code == int(http_response_code)
        print("Assert HTTP Response Code: %s == %s -- %s" % (result_response_code, http_response_code, assert_result))

    @staticmethod
    def process_assertions_for_test(result, test_args):
        if 'assert_result_type' in test_args:
            DatimImapTests.assert_result_type(result, test_args.get('assert_result_type'))
        if 'assert_http_response_code' in test_args:
            DatimImapTests.assert_http_response_code(result, test_args.get('assert_http_response_code'))
        if 'assert_result_value' in test_args:
            DatimImapTests.assert_result_value(result, test_args.get('assert_result_value'))
        if 'assert_num_diff' in test_args:
            DatimImapTests.assert_num_diff(result, test_args.get('assert_num_diff'))

    def run_test(self, test_args):
        """ Run a test """

        # Skip if set to inactive
        if "is_active" in test_args and not test_args["is_active"]:
            print('SKIPPING: "is_active" set to False')
            return

        # Pre-process args
        test_type = test_args['test_type']
        if 'country_org' not in test_args:
            test_args['country_org'] = 'DATIM-MOH-%s-%s' % (test_args.get('country_code'), test_args.get('period'))

        # Make sure its a valid test type
        print(self.get_test_summary(test_args))
        if test_type not in DatimImapTests.DATIM_OCL_TEST_TYPES:
            raise Exception('Invalid test_type "%s" with args: %s' % (test_type, test_args))

        # Run the test and save the result
        if self.__timer and self.__timer.running:
            self.__timer.lap(label='%s--Start' % test_args['test_id'])
        result = None
        try:
            if test_type == DatimImapTests.DATIM_OCL_TEST_TYPE_EXPORT:
                result = self.run_test_export(test_args)
            elif test_type == DatimImapTests.DATIM_OCL_TEST_TYPE_IMPORT:
                result = self.run_test_import(test_args)
            elif test_type == DatimImapTests.DATIM_OCL_TEST_TYPE_COMPARE:
                result = self.run_test_compare(test_args)
            elif test_type == DatimImapTests.DATIM_OCL_TEST_TYPE_DROP_ORG:
                result = self.run_test_drop_org(test_args)
        except Exception as e:
            result = e
        finally:
            self.results[test_args['test_id']] = result

        # Process the assertions
        DatimImapTests.process_assertions_for_test(result, test_args)

        # Optionally display the result
        if "do_display_result" not in test_args or test_args["do_display_result"]:
            print(result)

        # Record the lap time
        if self.__timer and self.__timer.running:
            self.__timer.lap(label='%s--Stop' % test_args['test_id'])

    def display_test_results(self):
        if not self.__tests:
            return False
        self.__current_test_num = 0
        for test in self.__tests:
            self.__current_test_num += 1
            print(self.get_test_summary(test))
            if test['test_id'] in self.results:
                result = self.results[test['test_id']]
                DatimImapTests.process_assertions_for_test(result, test)
            else:
                print('INFO: No result for test "%s"' % test['test_id'])
        if self.__timer:
            print('\n**** SUMMARY OF PROCESSING TIMES:')
            print(self.__timer)

    def run_tests(self, tests):
        self.__timer = utils.timer.Timer()
        self.__tests = list(tests)
        self.__current_test_num = 0
        self.__total_test_count = len(tests)
        self.__timer.start()
        for test in self.__tests:
            self.__current_test_num += 1
            self.run_test(test_args=test)
        self.__timer.stop()

    @staticmethod
    def display_test_summary(tests):
        i = 0
        for test in tests:
            i += 1
            print("[Test %s of %s] %s('%s'): %s" % (
                i, len(tests), test["test_type"], test["test_id"], test["test_description"]))

