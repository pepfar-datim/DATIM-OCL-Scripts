"""
DATIM I-MAP object and its helper classes
"""
import sys
import StringIO
import csv
import json
import re
import requests
import pprint
from operator import itemgetter
import deepdiff
import datimimapexport
import datimbase
import ocldev.oclcsvtojsonconverter


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

    def __init__(self, country_code='', country_org='', country_name='', period='',
                 imap_data=None, do_add_columns_to_csv=True):
        self.country_code = country_code
        self.country_org = country_org
        self.country_name = country_name
        self.period = period
        self.do_add_columns_to_csv = do_add_columns_to_csv
        self.__imap_data = None
        self.set_imap_data(imap_data)

    def __iter__(self):
        self._current_iter = 0
        return self

    def next(self):
        if self._current_iter >= len(self.__imap_data):
            raise StopIteration
        else:
            self._current_iter += 1
            if self.do_add_columns_to_csv:
                return self.add_columns_to_row(self.__imap_data[self._current_iter - 1].copy())
            else:
                return self.__imap_data[self._current_iter - 1].copy()

    def add_columns_to_row(self, row):
        """ Create the additional columns used in processing """
        row['DATIM_Disag_Name_Clean'] = ''
        row['Country Collection Name'] = ''
        row['Country Collection ID'] = ''
        row['DATIM From Concept URI'] = ''
        row['DATIM To Concept URI'] = ''
        row['Country Map Type'] = ''
        row['Country From Concept URI'] = ''
        row['Country To Concept URI'] = ''
        if row['MOH_Indicator_ID'] and row['MOH_Disag_ID']:
            datim_owner_type_url_part = datimbase.DatimBase.owner_type_to_stem(datimbase.DatimBase.datim_owner_type)
            country_owner_type_url_part = datimbase.DatimBase.owner_type_to_stem(datimbase.DatimBase.country_owner_type)
            row['DATIM_Disag_Name_Clean'] = '_'.join(row['DATIM_Disag_Name'].replace('>', ' gt ').replace('<', ' lt ').replace('|',' ').replace('+', ' plus ').split())
            row['Country Collection Name'] = row['DATIM_Indicator_ID'] + ': ' + row['DATIM_Disag_Name']
            # TODO: This is hardcoded right now -- bad form!
            if row['DATIM_Disag_ID'] == 'HllvX50cXC0':
                row['Country Collection ID'] = (row['DATIM_Indicator_ID'] + '_Total').replace('_', '-')
            else:
                row['Country Collection ID'] = (row['DATIM_Indicator_ID'] + '_' + row['DATIM_Disag_Name_Clean']).replace('_', '-')
            row['DATIM From Concept URI'] = '/%s/%s/sources/%s/concepts/%s/' % (datim_owner_type_url_part, datimbase.DatimBase.datim_owner_id, datimbase.DatimBase.datim_source_id, row['DATIM_Indicator_ID'])
            row['DATIM To Concept URI'] = '/%s/%s/sources/%s/concepts/%s/' % (datim_owner_type_url_part, datimbase.DatimBase.datim_owner_id, datimbase.DatimBase.datim_source_id, row['DATIM_Disag_ID'])
            row['Country Map Type'] = row['Operation'] + ' OPERATION'
            row['Country From Concept URI'] = '/%s/%s/sources/%s/concepts/%s/' % (country_owner_type_url_part, self.country_org, datimbase.DatimBase.country_source_id, row['MOH_Indicator_ID'])
            if row['MOH_Disag_ID'] == datimbase.DatimBase.NULL_DISAG_ID:
                # null_disag is stored in the PEPFAR/DATIM-MOH source, not the country source
                row['Country To Concept URI'] = '/%s/%s/sources/%s/concepts/%s/' % (datim_owner_type_url_part, datimbase.DatimBase.datim_owner_id, datimbase.DatimBase.datim_source_id, row['MOH_Disag_ID'])
            else:
                row['Country To Concept URI'] = '/%s/%s/sources/%s/concepts/%s/' % (country_owner_type_url_part, self.country_org, datimbase.DatimBase.country_source_id, row['MOH_Disag_ID'])
        return row

    @staticmethod
    def get_format_from_string(format_string, default_fmt='CSV'):
        for fmt in DatimImap.DATIM_IMAP_FORMATS:
            if format_string.lower() == fmt.lower():
                return fmt
        return default_fmt

    def get_imap_data(self, sort=False, exclude_empty_maps=False, convert_to_dict=False):
        """
        Get IMAP data.
        :param sort: Returns sorted list if True. Ignored if convert_to_dict is True.
        :param exclude_empty_maps: Rows with empty maps are excluded from the results if True.
        :param convert_to_dict: Returns a dictionary with a unique key for each row if True.
        :return: <list> or <dict>
        """
        data = self.__imap_data
        if exclude_empty_maps:
            new_data = []
            for row in data:
                if (row['DATIM_Indicator_ID'] and row['DATIM_Disag_ID'] and
                        row['Operation'] and row['MOH_Indicator_ID'] and row['MOH_Disag_ID']):
                    new_data.append(row.copy())
            data = new_data
        if sort:
            data = DatimImap.multikeysort(data, self.IMAP_FIELD_NAMES)
        if convert_to_dict:
            new_data = {}
            for row in data:
                new_data[DatimImap.get_imap_row_key(row, self.country_org)] = row
            data = new_data
        return data

    def get_imap_row_by_key(self, row_key):
        row_key_dict = DatimImap.parse_imap_row_key(row_key)
        for row in self.__imap_data:
            if (row['DATIM_Indicator_ID'] == row_key_dict['DATIM_Indicator_ID'] and
                    row['DATIM_Disag_ID'] == row_key_dict['DATIM_Disag_ID'] and
                    row['MOH_Indicator_ID'] == row_key_dict['MOH_Indicator_ID'] and
                    row['MOH_Disag_ID'] == row_key_dict['MOH_Disag_ID']):
                return row
        return None

    @staticmethod
    def get_imap_row_key(row, country_org):
        data = [
            'DATIM-MOH',
            row['DATIM_Indicator_ID'],
            row['DATIM_Disag_ID'],
            row['Operation'],
            country_org,
            row['MOH_Indicator_ID'],
            row['MOH_Disag_ID']
        ]
        si = StringIO.StringIO()
        cw = csv.writer(si)
        cw.writerow(data)
        return si.getvalue().strip('\r\n')

    @staticmethod
    def parse_imap_row_key(row_key):
        si = StringIO.StringIO(row_key)
        reader = csv.reader(si, delimiter=',')
        for row in reader:
            return {
                'DATIM_Org': row[0],
                'DATIM_Indicator_ID': row[1],
                'DATIM_Disag_ID': row[2],
                'Operation': row[3],
                'MOH_Org': row[4],
                'MOH_Indicator_ID': row[5],
                'MOH_Disag_ID': row[6],
            }
        return {}

    def length(self):
        if self.__imap_data:
            return len(self.__imap_data)
        return 0

    def set_imap_data(self, imap_data):
        self.__imap_data = []
        if isinstance(imap_data, csv.DictReader):
            for row in imap_data:
                self.__imap_data.append({k:unicode(v) for k,v in row.items()})
        elif type(imap_data) == type([]):
            for row in imap_data:
                self.__imap_data.append({k:unicode(v) for k,v in row.items()})
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

    def display(self, fmt=DATIM_IMAP_FORMAT_CSV, exclude_empty_maps=False):
        fmt = DatimImap.get_format_from_string(fmt)
        if fmt == self.DATIM_IMAP_FORMAT_CSV:
            writer = csv.DictWriter(sys.stdout, fieldnames=self.IMAP_FIELD_NAMES)
            writer.writeheader()
            for row in self.__imap_data:
                if exclude_empty_maps:
                    if (not row['Operation'] or not row['MOH_Indicator_ID'] or not row['MOH_Disag_ID'] or
                            not row['DATIM_Indicator_ID'] or not row['DATIM_Disag_ID']):
                        continue
                writer.writerow({k:v.encode('utf8') for k,v in row.items()})
        elif fmt == self.DATIM_IMAP_FORMAT_JSON:
            print(json.dumps(self.__imap_data))

    def diff(self, imap, exclude_empty_maps=False):
        return DatimImapDiff(self, imap, exclude_empty_maps=exclude_empty_maps)

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

    def has_country_indicator(self, indicator_id='', indicator_name=''):
        for row in self.__imap_data:
            if ((not indicator_id or (indicator_id and indicator_id == row['MOH_Indicator_ID'])) and
                    (not indicator_name or (indicator_name and indicator_name == row['MOH_Indicator_Name']))):
                return True
        return False

    def has_country_disag(self, disag_id='', disag_name=''):
        for row in self.__imap_data:
            if ((not disag_id or (disag_id and disag_id == row['MOH_Disag_ID'])) and
                    (not disag_name or (disag_name and disag_name == row['MOH_Disag_Name']))):
                return True
        return False

    def has_country_collection(self, csv_row_needle):
        full_csv_row_needle = self.add_columns_to_row(csv_row_needle.copy())
        needle_collection_id = full_csv_row_needle['Country Collection ID']
        if needle_collection_id:
            for csv_row_haystack in self.__imap_data:
                full_csv_row_haystack = self.add_columns_to_row(csv_row_haystack.copy())
                if full_csv_row_haystack['Country Collection ID'] == needle_collection_id:
                    return True
        return False

    def has_country_operation_mapping(self, csv_row):
        for row in self.__imap_data:
            if (row['DATIM_Indicator_ID'] == csv_row['DATIM_Indicator_ID'] and
                    row['DATIM_Disag_ID'] == csv_row['DATIM_Disag_ID'] and
                    row['MOH_Indicator_ID'] == csv_row['MOH_Indicator_ID'] and
                    row['MOH_Disag_ID'] == csv_row['MOH_Disag_ID']):
                return True
        return False

    def has_country_datim_mapping(self, csv_row):
        for row in self.__imap_data:
            if (row['DATIM_Indicator_ID'] == csv_row['DATIM_Indicator_ID'] and
                    row['DATIM_Disag_ID'] == csv_row['DATIM_Disag_ID']):
                return True
        return False

    def get_country_indicator_update_json(self, row):
        defs = [DatimMohCsvToJsonConverter.CSV_RESOURCE_DEF_MOH_INDICATOR]
        return DatimImapFactory.generate_import_script_from_csv_row(
            imap_input=self, csv_row=row, defs=defs)

    def get_country_indicator_create_json(self, row):
        defs = [DatimMohCsvToJsonConverter.CSV_RESOURCE_DEF_MOH_INDICATOR]
        return DatimImapFactory.generate_import_script_from_csv_row(
            imap_input=self, csv_row=row, defs=defs)

    def get_country_disag_update_json(self, row):
        defs = [DatimMohCsvToJsonConverter.CSV_RESOURCE_DEF_MOH_DISAG]
        return DatimImapFactory.generate_import_script_from_csv_row(
            imap_input=self, csv_row=row, defs=defs)

    def get_country_disag_create_json(self, row):
        defs = [DatimMohCsvToJsonConverter.CSV_RESOURCE_DEF_MOH_DISAG]
        return DatimImapFactory.generate_import_script_from_csv_row(
            imap_input=self, csv_row=row, defs=defs)

    def get_country_collection_create_json(self, row):
        defs = [DatimMohCsvToJsonConverter.CSV_RESOURCE_DEF_MOH_COLLECTION]
        return DatimImapFactory.generate_import_script_from_csv_row(
            imap_input=self, csv_row=row, defs=defs)

    def get_country_operation_mapping_create_json(self, row):
        defs = [DatimMohCsvToJsonConverter.CSV_RESOURCE_DEF_MOH_OPERATION_MAPPING]
        return DatimImapFactory.generate_import_script_from_csv_row(
            imap_input=self, csv_row=row, defs=defs)

    def get_country_datim_mapping_create_json(self, row):
        defs = [DatimMohCsvToJsonConverter.CSV_RESOURCE_DEF_MOH_DATIM_MAPPING]
        return DatimImapFactory.generate_import_script_from_csv_row(
            imap_input=self, csv_row=row, defs=defs)

    def get_country_operation_mapping_retire_json(self, row):
        # TODO
        return []


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
    def get_period_from_version_id(version_id):
        if DatimImapFactory.is_valid_period_version_id(version_id):
            return version_id[:version_id.find('.')]
        return ''

    @staticmethod
    def get_minor_version_from_version_id(version_id):
        if DatimImapFactory.is_valid_period_version_id(version_id):
            return version_id[version_id.find('.') + 1:]
        return ''

    @staticmethod
    def is_valid_period_version_id(version_id):
        period_position = version_id.find('.')
        if period_position > 0 and len(version_id) > 2 and len(version_id) - period_position > 1:
            return True
        return False

    @staticmethod
    def get_repo_latest_period_version(repo_url='', period='', oclapitoken='', released=True):
        """
        Returns the OCL repo version dictionary for the latest minor version of the specified period.
        If no period is specified, the most recent one is used. By default, only released repo versions
        are considered. Set released to False to consider all versions. This method requires that
        repo version results are sorted by date of creation in descending order.
        """
        oclapiheaders = {
            'Authorization': 'Token ' + oclapitoken,
            'Content-Type': 'application/json'
        }
        repo_versions_url = '%sversions/?limit=100' % (repo_url)
        r = requests.get(repo_versions_url, headers=oclapiheaders)
        r.raise_for_status()
        repo_versions = r.json()
        if repo_versions:
            for repo_version in repo_versions:
                if released and not repo_version['released']:
                    continue
                if not DatimImapFactory.is_valid_period_version_id(repo_version['id']):
                    continue
                if not period:
                    period = DatimImapFactory.get_period_from_version_id(repo_version['id'])
                current_period = DatimImapFactory.get_period_from_version_id(repo_version['id'])
                if period == current_period:
                    return repo_version
        return None

    @staticmethod
    def load_imap_from_csv(csv_filename='', country_code='', country_org='', country_name='', period=''):
        """ Load IMAP from CSV file """
        with open(csv_filename, 'rb') as input_file:
            imap_data = csv.DictReader(input_file)
            return DatimImap(imap_data=imap_data, country_code=country_code, country_name=country_name,
                             country_org=country_org, period=period)

    @staticmethod
    def load_imap_from_ocl(oclenv='', oclapitoken='', run_ocl_offline=False,
                           country_code='', country_org='', period='', verbosity=0):
        """ Fetch an IMAP from OCL. Returns none if country code/org is unrecognized """
        datim_imap_export = datimimapexport.DatimImapExport(
            oclenv=oclenv, oclapitoken=oclapitoken, run_ocl_offline=run_ocl_offline, verbosity=verbosity)
        return datim_imap_export.get_imap(
            period=period, country_org=country_org, country_code=country_code)

    @staticmethod
    def create_repo_version(oclenv='', oclapitoken='', repo_endpoint='', repo_version_id=''):
        """ Create a new repository version """
        oclapiheaders = {
            'Authorization': 'Token ' + oclapitoken,
            'Content-Type': 'application/json'
        }
        new_version_data = {
            'id': repo_version_id,
            'description': 'Automatically generated version',
            'released': True
        }
        repo_version_url = '%s%sversions/' % (oclenv, repo_endpoint)
        r = requests.post(
            repo_version_url, data=json.dumps(new_version_data), headers=oclapiheaders)
        r.raise_for_status()

    @staticmethod
    def generate_import_script_from_diff(imap_diff):
        """ Return a list of JSON imports representing the diff """
        import_list = []
        import_list_narrative = []
        diff_data = imap_diff.get_diff()

        # Handle 'dictionary_item_added' - new country mapping
        if 'dictionary_item_added' in diff_data:
            for diff_key in diff_data['dictionary_item_added'].keys():
                row_key = diff_key.strip("root['").strip("']")
                csv_row = diff_data['dictionary_item_added'][diff_key]

                # country indicator
                country_indicator_id = csv_row['MOH_Indicator_ID']
                country_indicator_name = csv_row['MOH_Indicator_Name']
                if imap_diff.imap_a.has_country_indicator(
                        indicator_id=country_indicator_id, indicator_name=country_indicator_name):
                    # do nothing
                    pass
                elif imap_diff.imap_a.has_country_indicator(indicator_id=country_indicator_id):
                    # update
                    import_list_narrative.append('Update country indicator: %s, %s' % (
                        country_indicator_id, country_indicator_name))
                    import_list += imap_diff.imap_b.get_country_indicator_update_json(csv_row)
                else:
                    # new
                    import_list_narrative.append('Create new country indicator: %s, %s' % (
                        country_indicator_id, country_indicator_name))
                    import_list += imap_diff.imap_b.get_country_indicator_create_json(csv_row)

                # country disag
                country_disag_id = csv_row['MOH_Disag_ID']
                country_disag_name = csv_row['MOH_Disag_Name']
                if imap_diff.imap_a.has_country_disag(
                        disag_id=country_disag_id, disag_name=country_disag_name):
                    # do nothing
                    pass
                elif imap_diff.imap_a.has_country_disag(disag_id=country_disag_id):
                    # update
                    import_list_narrative.append('Update country disag: %s, %s' % (
                        country_disag_id, country_disag_name))
                    import_list += imap_diff.imap_b.get_country_disag_update_json(csv_row)
                else:
                    # new
                    import_list_narrative.append('Create new country disag: %s, %s' % (
                        country_disag_id, country_disag_name))
                    import_list += imap_diff.imap_b.get_country_disag_create_json(csv_row)

                # country collection
                if not imap_diff.imap_a.has_country_collection(csv_row):
                    full_csv_row = imap_diff.imap_b.add_columns_to_row(csv_row.copy())
                    import_list_narrative.append('Create country collection: %s' % (
                        full_csv_row['Country Collection ID']))
                    import_list += imap_diff.imap_b.get_country_collection_create_json(csv_row)

                # country DATIM mapping
                if not imap_diff.imap_a.has_country_datim_mapping(csv_row):
                    import_list_narrative.append('Create country DATIM mapping: %s, %s --> %s --> %s, %s' % (
                        csv_row['DATIM_Indicator_ID'], csv_row['DATIM_Indicator_Name'], 'HAS DATIM OPTION',
                        csv_row['DATIM_Disag_ID'], csv_row['DATIM_Disag_Name']))
                    import_list += imap_diff.imap_b.get_country_datim_mapping_create_json(csv_row)

                # country operation mapping
                if not imap_diff.imap_a.has_country_operation_mapping(csv_row):
                    import_list_narrative.append('Create country operation mapping: %s, %s --> %s --> %s, %s' % (
                        csv_row['MOH_Indicator_ID'], csv_row['MOH_Indicator_Name'], csv_row['Operation'],
                        csv_row['MOH_Disag_ID'], csv_row['MOH_Disag_Name']))
                    import_list += imap_diff.imap_b.get_country_operation_mapping_create_json(csv_row)

        # Handle 'dictionary_item_removed' - removed country mapping
        if 'dictionary_item_removed' in diff_data:
            for diff_key in diff_data['dictionary_item_removed'].keys():
                row_key = diff_key.strip("root['").strip("']")
                csv_row = imap_diff.imap_a.get_imap_row_by_key(row_key)

                # TODO: country operation mapping
                # print 'dictionary_item_removed:', diff_key
                if imap_diff.imap_a.has_country_operation_mapping(csv_row):
                    import_list_narrative.append('SKIP: Retire country operation mapping: %s, %s --> %s --> %s, %s' % (
                        csv_row['MOH_Indicator_ID'], csv_row['MOH_Indicator_Name'], csv_row['Operation'],
                        csv_row['MOH_Disag_ID'], csv_row['MOH_Disag_Name']))
                    # import_list += imap_diff.imap_a.get_country_operation_mapping_retire_json(csv_row)

                # TODO: country disag
                """
                -- Ignoring for now, because the compare needs to be against OCL itself, not the IMAP object
                Is country disag used by any mappings that are not in the removed list? 
                If no, retire the country disag
                """
                country_disag_id = csv_row['MOH_Disag_ID']
                country_disag_name = csv_row['MOH_Disag_ID']
                if imap_diff.imap_a.has_country_disag(disag_id=country_disag_id, disag_name=country_disag_name):
                    import_list_narrative.append('SKIP: Retire country disag: %s, %s' % (
                        country_disag_id, country_disag_name))
                    # import_list += imap_diff.imap_a.get_country_disag_retire_json(csv_row)

                # TODO: country indicator
                """
                -- Ignoring for now, because the compare needs to be against OCL itself, not the IMAP object
                Is country indicator used by any mappings that are not in the removed list?
                If no, retire the country indicator
                """
                country_indicator_id = csv_row['MOH_Indicator_ID']
                country_indicator_name = csv_row['MOH_Indicator_ID']
                if imap_diff.imap_a.has_country_indicator(indicator_id=country_indicator_id, indicator_name=country_indicator_name):
                    import_list_narrative.append('SKIP: Retire country indicator: %s, %s' % (
                        country_indicator_id, country_indicator_name))
                    # import_list += imap_diff.imap_a.get_country_indicator_retire_json(csv_row)

                # TODO: country DATIM mapping
                """
                -- Ignoring for now, because the compare needs to be against OCL itself, not the IMAP object
                Is country collection still active? i.e. are there any mappings in this collection that 
                are not in the removed list? If no, retire the DATIM mapping
                """

        # Handle 'values_changed' - updated name for country indicator or disag
        if 'values_changed' in diff_data:
            for diff_key in diff_data['values_changed'].keys():
                regex_pattern = r"^root\[\'([a-zA-Z0-9\-_]+,[a-zA-Z0-9\-_]+,[a-zA-Z0-9\-_]+,[a-zA-Z0-9\-_]+,[a-zA-Z0-9\-_]+,[a-zA-Z0-9\-_]+,[a-zA-Z0-9\-_]+)\'\]\[\'(MOH_Disag_Name|MOH_Indicator_Name)\'\]$"
                regex_result = re.match(regex_pattern, diff_key)
                if not regex_result:
                    continue
                row_key = regex_result.group(1)
                matched_field_name = regex_result.group(2)

                #csv_row_old = imap_diff.imap_a.get_imap_row_by_key(row_key)
                csv_row_new = imap_diff.imap_b.get_imap_row_by_key(row_key)

                # MOH_Indicator_Name
                if matched_field_name == 'MOH_Indicator_Name':
                    import_list_narrative.append('Update country indicator name: %s, %s' % (
                        csv_row_new['MOH_Indicator_ID'], csv_row_new['MOH_Indicator_Name']))
                    import_list += imap_diff.imap_b.get_country_indicator_update_json(csv_row_new)

                # MOH_Disag_Name
                if matched_field_name == 'MOH_Disag_Name':
                    import_list_narrative.append('Update country disag name: %s, %s' % (
                        csv_row_new['MOH_Disag_ID'], csv_row_new['MOH_Disag_Name']))
                    import_list += imap_diff.imap_b.get_country_disag_update_json(csv_row_new)

        # TODO: Dedup the import_list JSON, if needed

        pprint.pprint(import_list_narrative)
        return import_list

    @staticmethod
    def generate_import_script_from_csv_row(imap_input=None, csv_row=None, defs=None):
        """ Return a list of JSON imports representing the CSV row"""
        datim_csv_converter = DatimMohCsvToJsonConverter(input_list=[csv_row])
        datim_csv_resource_definitions = datim_csv_converter.get_country_csv_resource_definitions(
            country_owner=imap_input.country_org,
            country_owner_type=datimbase.DatimBase.country_owner_type,
            country_source=datimbase.DatimBase.country_source_id,
            datim_map_type=datimbase.DatimBase.map_type_country_has_option,
            defs=defs)
        datim_csv_converter.set_resource_definitions(datim_csv_resource_definitions)
        attr = {
            'datim_owner_type': 'Organization',
            'datim_owner': 'PEPFAR',
            'datim_source': 'DATIM-MOH',
            'country_code': imap_input.country_code,
            'country_owner_type': 'Organization',
            'country_owner': imap_input.country_org,
            'country_source': 'DATIM-Alignment-Indicators',
            'null_disag_owner_type': 'Organization',
            'null_disag_owner': 'PEPFAR',
            'null_disag_source': 'DATIM-MOH',
            'datim_map_type': 'DATIM HAS OPTION',
        }
        import_list = datim_csv_converter.process_by_definition(attr=attr)
        # Dedup the import list using list enumaration
        import_list_dedup = [i for n, i in enumerate(import_list) if i not in import_list[n + 1:]]
        return import_list_dedup


    @staticmethod
    def generate_import_script_from_csv(imap_input):
        """ Return a list of JSON imports representing the entire CSV """
        datim_csv_converter = DatimMohCsvToJsonConverter(input_list=imap_input.get_imap_data())
        datim_csv_resource_definitions = datim_csv_converter.get_country_csv_resource_definitions(
            country_owner=imap_input.country_org,
            country_owner_type=datimbase.DatimBase.country_owner_type,
            country_source=datimbase.DatimBase.country_source_id,
            datim_map_type=datimbase.DatimBase.map_type_country_has_option)
        datim_csv_converter.set_resource_definitions(datim_csv_resource_definitions)
        attr = {
            'datim_owner_type': 'Organization',
            'datim_owner': 'PEPFAR',
            'datim_source': 'DATIM-MOH',
            'country_code': imap_input.country_code,
            'country_owner_type': 'Organization',
            'country_owner': imap_input.country_org,
            'country_source': 'DATIM-Alignment-Indicators',
            'null_disag_owner_type': 'Organization',
            'null_disag_owner': 'PEPFAR',
            'null_disag_source': 'DATIM-MOH',
            'datim_map_type': 'DATIM HAS OPTION',
        }
        import_list = datim_csv_converter.process_by_definition(attr=attr)
        # Dedup the import list using list enumaration
        import_list_dedup = [i for n, i in enumerate(import_list) if i not in import_list[n + 1:]]
        return import_list_dedup

    @staticmethod
    def delete_collection_references(oclenv='', oclapitoken='', collection_endpoint=''):
        """ Create a new repository version """
        oclapiheaders = {
            'Authorization': 'Token ' + oclapitoken,
            'Content-Type': 'application/json'
        }
        collections_url = '%s%scollections/?limit=0' % (oclenv, owner_endpoint)

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
        if period in ('FY17', 'FY18'):
            return True
        return False


class DatimImapDiff(object):
    """ Object representing the diff between two IMAP objects """

    def __init__(self, imap_a, imap_b, exclude_empty_maps=False):
        self.diff(imap_a, imap_b, exclude_empty_maps=exclude_empty_maps)

    def diff(self, imap_a, imap_b, exclude_empty_maps=False):
        self.imap_a = imap_a
        self.imap_b = imap_b
        self.__diff_data = deepdiff.DeepDiff(
            imap_a.get_imap_data(sort=True, exclude_empty_maps=exclude_empty_maps, convert_to_dict=True),
            imap_b.get_imap_data(sort=True, exclude_empty_maps=exclude_empty_maps, convert_to_dict=True),
            verbose_level=2)

        # Remove the Total vs. default differences
        if 'values_changed' in self.__diff_data:
            for key in self.__diff_data['values_changed'].keys():
                if (self.__diff_data['values_changed'][key]['new_value'] == 'Total' and
                    self.__diff_data['values_changed'][key]['old_value'] == 'default'):
                    del(self.__diff_data['values_changed'][key])

    def get_diff(self):
        return self.__diff_data


class DatimMohCsvToJsonConverter(ocldev.oclcsvtojsonconverter.OclCsvToJsonConverter):
    ''' Extend to add a custom CSV pre-processor '''

    CSV_RESOURCE_DEF_MOH_INDICATOR = 'MOH-Indicator'
    CSV_RESOURCE_DEF_MOH_DISAG = 'MOH-Disaggregate'
    CSV_RESOURCE_DEF_MOH_DATIM_MAPPING = 'MOH-Datim-Mapping'
    CSV_RESOURCE_DEF_MOH_OPERATION_MAPPING = 'MOH-Mapping-Operation'
    CSV_RESOURCE_DEF_MOH_COLLECTION = 'MOH-Mapping-Collection'

    def get_owner_type_url_part(self, owner_type):
        if owner_type == 'Organization':
            return 'orgs'
        elif owner_type == 'User':
            return 'users'
        return ''

    def preprocess_csv_row(self, row, attr=None):
        ''' Create all of the additional columns '''
        if row['MOH_Indicator_ID'] and row['MOH_Disag_ID']:
            row['DATIM Owner Type'] = attr['datim_owner_type']
            row['DATIM Owner ID'] = attr['datim_owner']
            row['DATIM Source ID'] = attr['datim_source']
            datim_owner_type_url_part = self.get_owner_type_url_part(row['DATIM Owner Type'])
            row['Country Data Element Owner Type'] = attr['country_owner_type']
            row['Country Data Element Owner ID'] = attr['country_owner']
            row['Country Data Element Source ID'] = attr['country_source']
            country_data_element_owner_type_url_part = self.get_owner_type_url_part(row['Country Data Element Owner Type'])
            if row['MOH_Disag_ID'] == 'null_disag':
                row['Country Disaggregate Owner Type'] = attr['null_disag_owner_type']
                row['Country Disaggregate Owner ID'] = attr['null_disag_owner']
                row['Country Disaggregate Source ID'] = attr['null_disag_source']
            else:
                row['Country Disaggregate Owner Type'] = attr['country_owner_type']
                row['Country Disaggregate Owner ID'] = attr['country_owner']
                row['Country Disaggregate Source ID'] = attr['country_source']
            country_disaggregate_owner_type_url_part = self.get_owner_type_url_part(row['Country Disaggregate Owner Type'])
            row['DATIM_Disag_Name_Clean'] = '_'.join(row['DATIM_Disag_Name'].replace('>', ' gt ').replace('<', ' lt ').replace('|',' ').replace('+', ' plus ').split())
            row['Country Collection Name'] = row['DATIM_Indicator_ID'] + ': ' + row['DATIM_Disag_Name']
            # TODO: This is hardcoded right now -- bad form!
            if row['DATIM_Indicator_ID'] == 'HllvX50cXC0':
                row['Country Collection ID'] = (row['DATIM_Indicator_ID'] + '_Total').replace('_', '-')
            else:
                row['Country Collection ID'] = (row['DATIM_Indicator_ID'] + '_' + row['DATIM_Disag_Name_Clean']).replace('_', '-')
            row['DATIM From Concept URI'] = '/%s/%s/sources/%s/concepts/%s/' % (datim_owner_type_url_part, row['DATIM Owner ID'], row['DATIM Source ID'], row['DATIM_Indicator_ID'])
            row['DATIM To Concept URI'] = '/%s/%s/sources/%s/concepts/%s/' % (datim_owner_type_url_part, row['DATIM Owner ID'], row['DATIM Source ID'], row['DATIM_Disag_ID'])
            row['Country Map Type'] = row['Operation'] + ' OPERATION'
            # Data Element
            row['Country From Concept URI'] = '/%s/%s/sources/%s/concepts/%s/' % (country_data_element_owner_type_url_part, row['Country Data Element Owner ID'], row['Country Data Element Source ID'], row['MOH_Indicator_ID'])
            # Disaggregate
            row['Country To Concept URI'] = '/%s/%s/sources/%s/concepts/%s/' % (country_disaggregate_owner_type_url_part, row['Country Disaggregate Owner ID'], row['Country Disaggregate Source ID'], row['MOH_Disag_ID'])
        else:
            row['DATIM_Disag_Name_Clean'] = ''
            row['Country Collection Name'] = ''
            row['Country Collection ID'] = ''
            row['DATIM From Concept URI'] = ''
            row['DATIM To Concept URI'] = ''
            row['Country Map Type'] = ''
            row['Country From Concept URI'] = ''
            row['Country To Concept URI'] = ''
        return row

    @staticmethod
    def get_country_csv_resource_definitions(country_owner='', country_owner_type='',
                                             country_source='', datim_map_type='', defs=None):
        csv_resource_definitions = [
            {
                'definition_name': DatimMohCsvToJsonConverter.CSV_RESOURCE_DEF_MOH_INDICATOR,
                'is_active': True,
                'resource_type':'Concept',
                'id_column':'MOH_Indicator_ID',
                'skip_if_empty_column':'MOH_Indicator_ID',
                ocldev.oclcsvtojsonconverter.OclCsvToJsonConverter.DEF_CORE_FIELDS:[
                    {'resource_field':'concept_class', 'value':'Indicator'},
                    {'resource_field':'datatype', 'value':'Numeric'},
                    {'resource_field':'owner', 'column':'Country Data Element Owner ID'},
                    {'resource_field':'owner_type', 'column':'Country Data Element Owner Type'},
                    {'resource_field':'source', 'column':'Country Data Element Source ID'},
                ],
                ocldev.oclcsvtojsonconverter.OclCsvToJsonConverter.DEF_SUB_RESOURCES:{
                    'names':[
                        [
                            {'resource_field':'name', 'column':'MOH_Indicator_Name'},
                            {'resource_field':'locale', 'value':'en'},
                            {'resource_field':'locale_preferred', 'value':'True'},
                            {'resource_field':'name_type', 'value':'Fully Specified'},
                        ],
                    ],
                    'descriptions':[]
                },
            },
            {
                'definition_name': DatimMohCsvToJsonConverter.CSV_RESOURCE_DEF_MOH_DISAG,
                'is_active': True,
                'resource_type':'Concept',
                'id_column':'MOH_Disag_ID',
                'skip_if_empty_column':'MOH_Disag_ID',
                ocldev.oclcsvtojsonconverter.OclCsvToJsonConverter.DEF_CORE_FIELDS:[
                    {'resource_field':'concept_class', 'value':'Disaggregate'},
                    {'resource_field':'datatype', 'value':'None'},
                    {'resource_field':'owner', 'column':'Country Disaggregate Owner ID'},
                    {'resource_field':'owner_type', 'column':'Country Disaggregate Owner Type'},
                    {'resource_field':'source', 'column':'Country Disaggregate Source ID'},
                ],
                ocldev.oclcsvtojsonconverter.OclCsvToJsonConverter.DEF_SUB_RESOURCES:{
                    'names':[
                        [
                            {'resource_field':'name', 'column':'MOH_Disag_Name'},
                            {'resource_field':'locale', 'value':'en'},
                            {'resource_field':'locale_preferred', 'value':'True'},
                            {'resource_field':'name_type', 'value':'Fully Specified'},
                        ],
                    ],
                    'descriptions':[]
                },
            },
            {
                'definition_name': DatimMohCsvToJsonConverter.CSV_RESOURCE_DEF_MOH_DATIM_MAPPING,
                'is_active': True,
                'resource_type':'Mapping',
                'id_column':None,
                'skip_if_empty_column':'MOH_Disag_ID',
                'internal_external': {'value':ocldev.oclcsvtojsonconverter.OclCsvToJsonConverter.INTERNAL_MAPPING_ID},
                ocldev.oclcsvtojsonconverter.OclCsvToJsonConverter.DEF_CORE_FIELDS:[
                    {'resource_field':'from_concept_url', 'column':'DATIM From Concept URI'},
                    {'resource_field':'map_type', 'value':datim_map_type},
                    {'resource_field':'to_concept_url', 'column':'DATIM To Concept URI'},
                    {'resource_field':'owner', 'value':country_owner},
                    {'resource_field':'owner_type', 'value':country_owner_type},
                    {'resource_field':'source', 'value':country_source},
                ]
            },
            {
                'definition_name': DatimMohCsvToJsonConverter.CSV_RESOURCE_DEF_MOH_OPERATION_MAPPING,
                'is_active': True,
                'resource_type': 'Mapping',
                'id_column': None,
                'skip_if_empty_column': 'Operation',
                'internal_external': {'value':ocldev.oclcsvtojsonconverter.OclCsvToJsonConverter.INTERNAL_MAPPING_ID},
                ocldev.oclcsvtojsonconverter.OclCsvToJsonConverter.DEF_CORE_FIELDS:[
                    {'resource_field':'from_concept_url', 'column':'Country From Concept URI'},
                    {'resource_field':'map_type', 'column':'Country Map Type'},
                    {'resource_field':'to_concept_url', 'column':'Country To Concept URI'},
                    {'resource_field':'owner', 'value':country_owner},
                    {'resource_field':'owner_type', 'value':country_owner_type},
                    {'resource_field':'source', 'value':country_source},
                ]
            },
            {
                'definition_name': DatimMohCsvToJsonConverter.CSV_RESOURCE_DEF_MOH_COLLECTION,
                'is_active': True,
                'resource_type': 'Collection',
                'id_column': 'Country Collection ID',
                'skip_if_empty_column': 'Country Collection ID',
                ocldev.oclcsvtojsonconverter.OclCsvToJsonConverter.DEF_CORE_FIELDS: [
                    {'resource_field':'full_name', 'column':'Country Collection Name'},
                    {'resource_field':'name', 'column':'Country Collection Name'},
                    {'resource_field':'short_code', 'column':'Country Collection ID'},
                    {'resource_field':'collection_type', 'value':'Subset'},
                    {'resource_field':'supported_locales', 'value':'en'},
                    {'resource_field':'public_access', 'value':'View'},
                    {'resource_field':'default_locale', 'value':'en'},
                    {'resource_field':'description', 'value':''},
                    {'resource_field':'owner', 'value':country_owner},
                    {'resource_field':'owner_type', 'value':country_owner_type},
                ]
            }
        ]
        if defs:
            for csv_definition in csv_resource_definitions:
                if csv_definition['definition_name'] not in defs:
                    csv_definition['is_active'] = False
        return csv_resource_definitions

