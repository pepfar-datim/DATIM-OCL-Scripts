"""
DATIM I-MAP object and its helper classes
"""
import sys
import csv
import json
from operator import itemgetter
import deepdiff
import datimimapexport


class DatimImap(object):
    """
    Object representing a set of country indicator mappings
    """

    IMAP_FIELD_NAMES = [
        'DATIM_Indicator_Category',
        'DATIM_Indicator_ID',
        'DATIM_Disag_ID',
        'DATIM_Disag_Name',
        'Operation',
        'MOH_Indicator_ID',
        'MOH_Indicator_Name',
        'MOH_Disag_ID',
        'MOH_Disag_Name',
    ]

    DATIM_IMAP_FORMAT_CSV = 'CSV'
    DATIM_IMAP_FORMAT_JSON = 'JSON'
    DATIM_IMAP_FORMATS = [
        DATIM_IMAP_FORMAT_CSV,
        DATIM_IMAP_FORMAT_JSON,
    ]

    def __init__(self, country_code='', country_org='', country_name='', period='', imap_data=None):
        self.country_code = country_code
        self.country_org = country_org
        self.country_name = country_name
        self.period = period
        self.__imap_data = None
        self.set_imap_data(imap_data)

    @staticmethod
    def get_format_from_string(format_string, default_fmt='CSV'):
        for fmt in DatimImap.DATIM_IMAP_FORMATS:
            if format_string.lower() == fmt.lower():
                return fmt
        return default_fmt

    def get_imap_data(self):
        return self.__imap_data

    def get_sorted_imap_data(self):
        return DatimImap.multikeysort(self.__imap_data, self.IMAP_FIELD_NAMES)

    def set_imap_data(self, imap_data):
        self.__imap_data = []
        if isinstance(imap_data, csv.DictReader):
            for row in imap_data:
                self.__imap_data.append({k:unicode(v) for k,v in row.items()})
        elif type(imap_data) == type([]):
            for row in imap_data:
                self.__imap_data.append({k:unicode(v) for k,v in row.items()})
            #self.__imap_data = imap_data
        else:
            raise Exception("Cannot set I-MAP data with '%s'" % imap_data)

    def is_valid(self, throw_exception_on_error=True):
        if self.__imap_data:
            line_number = 0
            for row in self.__imap_data:
                line_number += 1
                for field_name in self.IMAP_FIELD_NAMES:
                    if field_name not in row:
                        if throw_exception_on_error:
                            raise Exception("Missing field '%s' on row %s of input file" % (
                                field_name, line_number))
                        else:
                            return False
            return True
        return False

    def display(self, fmt=DATIM_IMAP_FORMAT_CSV):
        fmt = DatimImap.get_format_from_string(fmt)
        if fmt == self.DATIM_IMAP_FORMAT_CSV:
            writer = csv.DictWriter(sys.stdout, fieldnames=self.IMAP_FIELD_NAMES)
            writer.writeheader()
            for row in self.__imap_data:
                writer.writerow({k:v.encode('utf8') for k,v in row.items()})
        elif fmt == self.DATIM_IMAP_FORMAT_JSON:
            print(json.dumps(self.__imap_data))

    def diff(self, imap):
        return DatimImapDiff(self, imap)

    def get_csv(self):
        pass

    def get_ocl_collections(self):
        pass

    @staticmethod
    def multikeysort(items, columns):
        from operator import itemgetter
        comparers = [((itemgetter(col[1:].strip()), -1) if col.startswith('-') else
                      (itemgetter(col.strip()), 1)) for col in columns]
        def comparer(left, right):
            for fn, mult in comparers:
                result = cmp(fn(left), fn(right))
                if result:
                    return mult * result
            else:
                return 0
        return sorted(items, cmp=comparer)


class DatimImapFactory(object):
    @staticmethod
    def _convert_endpoint_to_filename_fmt(endpoint):
        filename = endpoint.replace('/', '-')
        if filename[0] == '-':
            filename = filename[1:]
        if filename[-1] == '-':
            filename = filename[:-1]
        return filename

    @staticmethod
    def endpoint2filename_ocl_export_zip(endpoint):
        return 'ocl-' + DatimImapFactory._convert_endpoint_to_filename_fmt(endpoint) + '.zip'

    @staticmethod
    def endpoint2filename_ocl_export_json(endpoint):
        return 'ocl-' + DatimImapFactory._convert_endpoint_to_filename_fmt(endpoint) + '-raw.json'

    @staticmethod
    def load_imap_from_csv(csv_filename='', country_code='', country_org='', country_name='', period=''):
        with open(csv_filename, 'rb') as input_file:
            imap_data = csv.DictReader(input_file)
            return DatimImap(imap_data=imap_data, country_code=country_code, country_name=country_name,
                             country_org=country_org, period=period)

    @staticmethod
    def load_imap_from_ocl(oclenv='', oclapitoken='', run_ocl_offline=False,
                           country_code='', country_org='', period=''):
        """ Fetch an IMAP from OCL """

        datim_imap_export = datimimapexport.DatimImapExport(
            oclenv=oclenv, oclapitoken=oclapitoken, run_ocl_offline=run_ocl_offline)
        return datim_imap_export.get_imap(
            period=period, country_org=country_org, country_code=country_code)

    @staticmethod
    def generate_import_script_from_diff(imap_diff):
        exit()

    @staticmethod
    def get_csv(datim_imap):
        pass

    @staticmethod
    def get_ocl_import_script(datim_imap):
        pass

    @staticmethod
    def get_ocl_import_script_from_diff(imap_diff):
        pass

    @staticmethod
    def is_valid_imap_period(period):
        # Confirm that the period has been defined in the PEPFAR metadata
        if period == 'FY17':
            return True
        return False


class DatimImapDiff(object):
    """ Object representing the diff between two IMAP objects """

    def __init__(self, imap_a, imap_b):
        self.imap_a = imap_a
        self.imap_b = imap_b
        self.diff(imap_a, imap_b)

    def diff(self, imap_a, imap_b):
        self.__diff_data = deepdiff.DeepDiff(
            imap_a.get_sorted_imap_data(), imap_b.get_sorted_imap_data(),
            verbose_level=2)

    def get_diff(self):
        return self.__diff_data
