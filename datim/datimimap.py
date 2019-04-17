"""
DATIM IMAP object and its helper classes
"""
import sys
import StringIO
import csv
import json
import re
import pprint
from operator import itemgetter
import requests
import deepdiff
import datimimapexport
import datimbase
import ocldev.oclcsvtojsonconverter


class DatimImap(object):
    """
    Object representing a set of country indicator mappings
    """

    # Required IMAP field names
    IMAP_FIELD_DATIM_INDICATOR_CATEGORY = 'DATIM_Indicator_Category'
    IMAP_FIELD_DATIM_INDICATOR_ID = 'DATIM_Indicator_ID'
    IMAP_FIELD_DATIM_DISAG_ID = 'DATIM_Disag_ID'
    IMAP_FIELD_DATIM_DISAG_NAME = 'DATIM_Disag_Name'
    IMAP_FIELD_OPERATION = 'Operation'
    IMAP_FIELD_MOH_INDICATOR_ID = 'MOH_Indicator_ID'
    IMAP_FIELD_MOH_INDICATOR_NAME = 'MOH_Indicator_Name'
    IMAP_FIELD_MOH_DISAG_ID = 'MOH_Disag_ID'
    IMAP_FIELD_MOH_DISAG_NAME = 'MOH_Disag_Name'
    IMAP_FIELD_NAMES = [
        IMAP_FIELD_DATIM_INDICATOR_CATEGORY,
        IMAP_FIELD_DATIM_INDICATOR_ID,
        IMAP_FIELD_DATIM_DISAG_ID,
        IMAP_FIELD_DATIM_DISAG_NAME,
        IMAP_FIELD_OPERATION,
        IMAP_FIELD_MOH_INDICATOR_ID,
        IMAP_FIELD_MOH_INDICATOR_NAME,
        IMAP_FIELD_MOH_DISAG_ID,
        IMAP_FIELD_MOH_DISAG_NAME,
    ]

    # Field names generated and used by OCL to support the import process
    IMAP_EXTRA_FIELD_NAMES = [
        'Country Collection ID',
        'Country Map Type',
        'Country Collection Name',
        'Country To Concept URI',
        'DATIM From Concept URI',
        'Country From Concept URI',
        'DATIM To Concept URI',
        'DATIM_Disag_Name_Clean',
        'DATIM Owner Type',
        'DATIM Owner ID',
        'DATIM Source ID',
        'DATIM Map Type',
        'Country Data Element Owner Type',
        'Country Data Element Owner ID',
        'Country Data Element Source ID',
        'Country Disaggregate Owner Type',
        'Country Disaggregate Owner ID',
        'Country Disaggregate Source ID',
    ]

    # Name of custom attribute in DATIM_MOH indicator concepts in OCL that contains the indicator category value
    # (e.g. HTS_TST). Note that this value must be set manually in OCL.
    IMAP_INDICATOR_CATEGORY_CUSTOM_ATTRIBUTE = 'indicator_category_code'

    # IMAP formats
    DATIM_IMAP_FORMAT_CSV = 'CSV'
    DATIM_IMAP_FORMAT_JSON = 'JSON'
    DATIM_IMAP_FORMAT_HTML = 'HTML'
    DATIM_IMAP_FORMATS = [
        DATIM_IMAP_FORMAT_CSV,
        DATIM_IMAP_FORMAT_JSON,
        DATIM_IMAP_FORMAT_HTML
    ]

    # Set to True to treat equal MOH_Indicator_ID and MOH_Disag_ID values in the same row as a null MOH disag
    SET_EQUAL_MOH_ID_TO_NULL_DISAG = False

    # NOT IMPLEMENTED - May use these to configure handling of null disags for individual IMAP resources
    DATIM_EMPTY_DISAG_MODE_NULL = 'null'
    DATIM_EMPTY_DISAG_MODE_BLANK = 'blank'
    DATIM_EMPTY_DISAG_MODE_RAW = 'raw'
    DATIM_EMPTY_DISAG_MODES = [
        DATIM_EMPTY_DISAG_MODE_NULL,
        DATIM_EMPTY_DISAG_MODE_BLANK,
        DATIM_EMPTY_DISAG_MODE_RAW
    ]

    def __init__(self, country_code='', country_org='', country_name='', period='',
                 imap_data=None, do_add_columns_to_csv=True):
        """ Constructor for DatimImap class """
        self.country_code = country_code
        self.country_org = country_org
        self.country_name = country_name
        self.period = period
        self.do_add_columns_to_csv = do_add_columns_to_csv
        self.__imap_data = None
        self.set_imap_data(imap_data)

    def __iter__(self):
        """ Iterator for the DatimImap class """
        self._current_iter = 0
        return self

    def next(self):
        """
        Iterator for the DatimImap class
        :return:
        """
        # TODO: Provide a way to customize how the rows are returned when doing this loop
        if self._current_iter >= len(self.__imap_data):
            raise StopIteration
        else:
            self._current_iter += 1
            return self.get_row(self._current_iter - 1, include_extra_info=self.do_add_columns_to_csv,
                                auto_fix_null_disag=True, convert_to_dict=False, exclude_empty_maps=False)

    @staticmethod
    def get_format_from_string(format_string, default_fmt='CSV'):
        """
        Get the DATIM_IMAP_FORMAT constant from a string
        :param format_string:
        :param default_fmt:
        :return:
        """
        for fmt in DatimImap.DATIM_IMAP_FORMATS:
            if format_string.lower() == fmt.lower():
                return fmt
        return default_fmt

    def get_row(self, row_number, include_extra_info=False, auto_fix_null_disag=True,
                convert_to_dict=False, exclude_empty_maps=False, show_null_disag_as_blank=False):
        """
        Returns the specified IMAP row in the requested format
        :param row_number: 0-based row number of the IMAP to return
        :param include_extra_info: Adds extra columns if True
        :param auto_fix_null_disag: Replace empty disags with 'null_disag' if True
        :param convert_to_dict: Returns the IMAP row as a dict with a unique row key if True
        :param exclude_empty_maps: Returns None if row represents an empty map
        :return: Returns list, dict, or None
        """
        row = self.__imap_data[row_number].copy()
        if row and exclude_empty_maps and DatimImap.is_empty_map(row):
            return None

        # Replace alternative null disag representations with the actual null disag concept
        if row and auto_fix_null_disag:
            row = DatimImap.fix_null_disag_in_row(row)

        if row and include_extra_info:
            row = self.add_columns_to_row(row)

        # Optionally replace null disags with blank disag ID/Name values
        if row and show_null_disag_as_blank and row[DatimImap.IMAP_FIELD_MOH_DISAG_ID] == datimbase.DatimBase.NULL_DISAG_ID:
            row = row.copy()
            row[DatimImap.IMAP_FIELD_MOH_DISAG_ID] = ''
            row[DatimImap.IMAP_FIELD_MOH_DISAG_NAME] = ''

        if row and convert_to_dict:
            return {DatimImap.get_imap_row_key(row, self.country_org): row}
        return row

    @staticmethod
    def is_empty_map(row):
        """
        Returns True if the row is considered an empty mapping; False otherwise.
        A row is considered a valid mapping if DATIM_Indicator_ID, DATIM_Disag_ID,
        Operation, and MOH_Indicator_ID are all set; otherwise, it is considered empty.
        :param row: <dict>
        :return: <bool>
        """
        if (row[DatimImap.IMAP_FIELD_DATIM_INDICATOR_ID] and row[DatimImap.IMAP_FIELD_DATIM_DISAG_ID] and
                row[DatimImap.IMAP_FIELD_OPERATION] and row[DatimImap.IMAP_FIELD_MOH_INDICATOR_ID]):
            return False
        return True

    @staticmethod
    def is_null_disag_row(row):
        """
        Returns True if specified row is a null disag row
        :param row: Row to be checked
        :return: bool
        """
        if not row[DatimImap.IMAP_FIELD_MOH_INDICATOR_ID]:
            return False
        if row[DatimImap.IMAP_FIELD_MOH_DISAG_ID] == datimbase.DatimBase.NULL_DISAG_ID or not row[DatimImap.IMAP_FIELD_MOH_DISAG_ID]:
            return True
        elif row[DatimImap.IMAP_FIELD_MOH_DISAG_ID] == row[DatimImap.IMAP_FIELD_MOH_INDICATOR_ID] and DatimImap.SET_EQUAL_MOH_ID_TO_NULL_DISAG:
            return True
        return False

    @staticmethod
    def fix_null_disag_in_row(row):
        """
        Sets disag to "null" if it is empty or, optionally, if indicator ID equals the disag ID
        :param row:
        :return:
        """
        if DatimImap.is_null_disag_row(row):
            row = row.copy()
            row[DatimImap.IMAP_FIELD_MOH_DISAG_ID] = datimbase.DatimBase.NULL_DISAG_ID
            row[DatimImap.IMAP_FIELD_MOH_DISAG_NAME] = datimbase.DatimBase.NULL_DISAG_NAME
        return row

    def get_imap_data(self, sort=False, exclude_empty_maps=False, convert_to_dict=False, include_extra_info=False,
                      auto_fix_null_disag=True, show_null_disag_as_blank=False):
        """
        Returns data for the entire IMAP based on the parameters sent
        :param sort: Returns sorted list if True. Ignored if convert_to_dict is True
        :param exclude_empty_maps: Rows with empty maps are excluded from the results if True
        :param convert_to_dict: Return a dictionary with a unique key for each row if True
        :param include_extra_info: Add extra pre-processing columns used for import into OCL
        :param auto_fix_null_disag: Replaces empty disags with 'null_disag' if True
        :return: <list> or <dict>
        """
        if convert_to_dict:
            data = {}
        else:
            data = []
        for row_number in range(self.length()):
            row = self.get_row(
                row_number, include_extra_info=include_extra_info, exclude_empty_maps=exclude_empty_maps,
                convert_to_dict=convert_to_dict, auto_fix_null_disag=auto_fix_null_disag,
                show_null_disag_as_blank=show_null_disag_as_blank)
            if not row:
                continue
            if convert_to_dict:
                data.update(row.copy())
            else:
                data.append(row.copy())
        if sort and not convert_to_dict:
            data = DatimImap.multikeysort(data, self.IMAP_FIELD_NAMES)
        return data

    @staticmethod
    def get_imap_row_key(row, country_org):
        """
        Returns a string representing the key of a row
        :param row:
        :param country_org:
        :return:
        """
        if row[DatimImap.IMAP_FIELD_MOH_INDICATOR_ID] and not row[DatimImap.IMAP_FIELD_MOH_DISAG_ID]:
            disag_id = datimbase.DatimBase.NULL_DISAG_ID
        else:
            disag_id = row[DatimImap.IMAP_FIELD_MOH_DISAG_ID]
        data = [
            'DATIM-MOH',
            row[DatimImap.IMAP_FIELD_DATIM_INDICATOR_ID],
            row[DatimImap.IMAP_FIELD_DATIM_DISAG_ID],
            row[DatimImap.IMAP_FIELD_OPERATION],
            country_org,
            row[DatimImap.IMAP_FIELD_MOH_INDICATOR_ID],
            disag_id
        ]
        si = StringIO.StringIO()
        cw = csv.writer(si)
        cw.writerow(data)
        return si.getvalue().strip('\r\n')

    def get_imap_row_by_key(
            self, row_key, include_extra_info=False, auto_fix_null_disag=True, convert_to_dict=False):
        """
        Return a specific row of the IMAP that matches the specified string row_key.
        Note that rows representing an empty map do not have keys and cannot be matched by this method.
        :param row_key:
        :return:
        """
        row_key_dict = DatimImap.parse_imap_row_key(row_key)
        if row_key_dict:
            for row_number in range(self.length()):
                row = self.get_row(row_number, exclude_empty_maps=True, auto_fix_null_disag=True)
                if not row:
                    continue
                if (row[DatimImap.IMAP_FIELD_DATIM_INDICATOR_ID] == row_key_dict[DatimImap.IMAP_FIELD_DATIM_INDICATOR_ID] and
                        row[DatimImap.IMAP_FIELD_DATIM_DISAG_ID] == row_key_dict[DatimImap.IMAP_FIELD_DATIM_DISAG_ID] and
                        row[DatimImap.IMAP_FIELD_MOH_INDICATOR_ID] == row_key_dict[DatimImap.IMAP_FIELD_MOH_INDICATOR_ID] and
                        row[DatimImap.IMAP_FIELD_MOH_DISAG_ID] == row_key_dict[DatimImap.IMAP_FIELD_MOH_DISAG_ID]):
                    # Request the row again applying the format attributes
                    return self.get_row(row_number, auto_fix_null_disag=auto_fix_null_disag,
                                        include_extra_info=include_extra_info, convert_to_dict=convert_to_dict)
        return None

    @staticmethod
    def parse_imap_row_key(row_key):
        si = StringIO.StringIO(row_key)
        reader = csv.reader(si, delimiter=',')
        for row in reader:
            return {
                'DATIM_Source': row[0],
                DatimImap.IMAP_FIELD_DATIM_INDICATOR_ID: row[1],
                DatimImap.IMAP_FIELD_DATIM_DISAG_ID: row[2],
                DatimImap.IMAP_FIELD_OPERATION: row[3],
                'MOH_Org': row[4],
                DatimImap.IMAP_FIELD_MOH_INDICATOR_ID: row[5],
                DatimImap.IMAP_FIELD_MOH_DISAG_ID: row[6],
            }
        return {}

    def length(self):
        """
        Returns the number of rows in the IMAP
        :return: Number of rows in the IMAP
        """
        if self.__imap_data:
            return len(self.__imap_data)
        return 0

    def set_imap_data(self, imap_data):
        """
        Sets the IMAP data, discarding unrecognized columns, and ensures unicode encoding
        :param imap_data:
        :return:
        """
        # TODO: Note the explicit UTF-8 character encoding and ignoring unicode decoding errors - fix in future
        self.__imap_data = []
        if isinstance(imap_data, csv.DictReader) or type(imap_data) == type([]):
            for row in imap_data:
                # Get rid of unrecognized columns and ensure unicode encoding
                row_to_save = {}
                for field_name in list(self.IMAP_FIELD_NAMES):
                    row_to_save[field_name] = DatimImap.uors2u(row[field_name], 'utf8', 'ignore')
                # Store the cleaned up row in this IMAP object
                self.__imap_data.append(row_to_save)
        else:
            raise Exception("Cannot set I-MAP data with '%s'" % imap_data)

    @staticmethod
    def uors2u(object, encoding='utf8', errors='strict'):
        """
        Safely convert object to unicode
        :param object: Object to convert
        :param encoding: If a string, character encoding of the text to decode
        :param errors: If a string, how to handle errors. See Python v2.7 docs for unicode() built-in function
        :return: <unicode>
        """
        if isinstance(object, unicode):
            return object
        try:
            return unicode(object, encoding, errors)
        except:
            return object

    def is_valid(self, throw_exception_on_error=True):
        """
        This method really needs some work...
        :param throw_exception_on_error:
        :return:
        """
        # TODO: Update DatimImap.is_valid to work with new get_row model
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

    def display(self, fmt=DATIM_IMAP_FORMAT_CSV, sort=False, exclude_empty_maps=False, include_extra_info=False,
                auto_fix_null_disag=False, show_null_disag_as_blank=True):
        """
        Outputs IMAP contents as CSV or JSON
        :param fmt: string CSV, JSON, HTML
        :param sort: default=False; Set to True to sort by DATIM indicator+disag followed by Country indicator+disag
        :param exclude_empty_maps: Rows with empty maps are excluded from the results if True.
        :param include_extra_info: Add extra pre-processing columns
        :param auto_fix_null_disag: Replaces empty disags with 'null_disag' if True
        :param show_null_disag_as_blank: Replaces null_disag with empty string if True
        :return: None
        """
        fmt = DatimImap.get_format_from_string(fmt)
        if fmt not in DatimImap.DATIM_IMAP_FORMATS:
            fmt = DatimImap.DATIM_IMAP_FORMAT_JSON
        data = self.get_imap_data(sort=sort, exclude_empty_maps=exclude_empty_maps,
                                  include_extra_info=include_extra_info, auto_fix_null_disag=auto_fix_null_disag,
                                  show_null_disag_as_blank=show_null_disag_as_blank)
        if fmt == self.DATIM_IMAP_FORMAT_CSV:
            fieldnames = list(self.IMAP_FIELD_NAMES)
            if include_extra_info:
                fieldnames += list(self.IMAP_EXTRA_FIELD_NAMES)
            writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
            writer.writeheader()
            for row in data:
                # throw out columns we don't need
                row_to_output = {}
                for field_name in fieldnames:
                    row_to_output[field_name] = row[field_name].encode('utf8')
                # output the row
                writer.writerow(row_to_output)
        elif fmt == self.DATIM_IMAP_FORMAT_JSON:
            print(json.dumps(data))
        elif fmt == self.DATIM_IMAP_FORMAT_HTML:
            print('<h1>Country IMAP Export for Country Code "%s" and Period "%s"</h1>' % (
                self.country_code, self.period))
            print('<table border="1" cellspacing="0"><tr>')
            for field_name in self.IMAP_FIELD_NAMES:
                print('<th>%s</th>' % field_name)
            print('</tr>')
            for row in data:
                print('<tr>')
                for field_name in self.IMAP_FIELD_NAMES:
                    print('<td>%s</td>' % row[field_name])
                print('</tr>')
            print('</table>')

    def diff(self, imap, exclude_empty_maps=True):
        """
        Get an object representing the diff between two IMAPs
        :param imap: The IMAP object to compare
        :param exclude_empty_maps: Set to True to exclude empty maps from the diff
        :return: DatimImapDiff
        """
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

    def add_columns_to_row(self, row):
        """
        Create the additional columns used in processing
        :param row: Row to add columns to
        :return: dict
        """

        # Start by adding empty string values for each extra field
        row = row.copy()
        for key in DatimImap.IMAP_EXTRA_FIELD_NAMES:
            row[key] = ''

        # Get out of here if no ID set for MOH Indicator
        if not row[DatimImap.IMAP_FIELD_MOH_INDICATOR_ID]:
            return row

        # Set DATIM attributes
        row['DATIM Owner Type'] = datimbase.DatimBase.datim_owner_type
        row['DATIM Owner ID'] = datimbase.DatimBase.datim_owner_id
        row['DATIM Source ID'] = datimbase.DatimBase.datim_source_id
        datim_owner_type_url_part = datimbase.DatimBase.owner_type_to_stem(row['DATIM Owner Type'])

        # Set country data element attributes
        row['Country Data Element Owner Type'] = datimbase.DatimBase.country_owner_type
        row['Country Data Element Owner ID'] = self.country_org
        row['Country Data Element Source ID'] = datimbase.DatimBase.country_source_id
        country_data_element_owner_type_url_part = datimbase.DatimBase.owner_type_to_stem(
            row['Country Data Element Owner Type'])

        # Set country disag attributes, handling the null disag case
        if DatimImap.is_null_disag_row(row):
            row['Country Disaggregate Owner Type'] = datimbase.DatimBase.NULL_DISAG_OWNER_TYPE
            row['Country Disaggregate Owner ID'] = datimbase.DatimBase.NULL_DISAG_OWNER_ID
            row['Country Disaggregate Source ID'] = datimbase.DatimBase.NULL_DISAG_SOURCE_ID
            moh_disag_id = datimbase.DatimBase.NULL_DISAG_ID
            moh_disag_name = datimbase.DatimBase.NULL_DISAG_NAME
        else:
            row['Country Disaggregate Owner Type'] = datimbase.DatimBase.country_owner_type
            row['Country Disaggregate Owner ID'] = self.country_org
            row['Country Disaggregate Source ID'] = datimbase.DatimBase.country_source_id
            moh_disag_id = row[DatimImap.IMAP_FIELD_MOH_DISAG_ID]
            moh_disag_name = row[DatimImap.IMAP_FIELD_MOH_DISAG_NAME]
        country_disaggregate_owner_type_url_part = datimbase.DatimBase.owner_type_to_stem(
            row['Country Disaggregate Owner Type'])

        # Build the collection name
        # TODO: The country collection name should only be used if a collection has not already been defined
        country_owner_type_url_part = datimbase.DatimBase.owner_type_to_stem(datimbase.DatimBase.country_owner_type)
        row['DATIM_Disag_Name_Clean'] = '_'.join(
            row[DatimImap.IMAP_FIELD_DATIM_DISAG_NAME].replace('>', ' gt ').replace('<', ' lt ').replace('|', ' ').replace('+', ' plus ').split())
        row['Country Collection Name'] = row[DatimImap.IMAP_FIELD_DATIM_INDICATOR_ID] + ': ' + row[DatimImap.IMAP_FIELD_DATIM_DISAG_NAME]

        # Build the collection ID, replacing the default disag ID from DHIS2 with plain English (i.e. Total)
        if row[DatimImap.IMAP_FIELD_DATIM_DISAG_ID] == datimbase.DatimBase.DATIM_DEFAULT_DISAG_ID:
            row['Country Collection ID'] = (
                row[DatimImap.IMAP_FIELD_DATIM_INDICATOR_ID] + '_' + datimbase.DatimBase.DATIM_DEFAULT_DISAG_REPLACEMENT_NAME).replace('_', '-')
        else:
            row['Country Collection ID'] = (
                row[DatimImap.IMAP_FIELD_DATIM_INDICATOR_ID] + '_' + row['DATIM_Disag_Name_Clean']).replace('_', '-')

        # DATIM mapping
        row['DATIM From Concept URI'] = '/%s/%s/sources/%s/concepts/%s/' % (
            datim_owner_type_url_part, datimbase.DatimBase.datim_owner_id,
            datimbase.DatimBase.datim_source_id, row[DatimImap.IMAP_FIELD_DATIM_INDICATOR_ID])
        row['DATIM To Concept URI'] = '/%s/%s/sources/%s/concepts/%s/' % (
            datim_owner_type_url_part, datimbase.DatimBase.datim_owner_id,
            datimbase.DatimBase.datim_source_id, row[DatimImap.IMAP_FIELD_DATIM_DISAG_ID])
        row['DATIM Map Type'] = datimbase.DatimBase.map_type_country_has_option

        # Country mapping
        row['Country Map Type'] = row[DatimImap.IMAP_FIELD_OPERATION] + ' OPERATION'
        row['Country From Concept URI'] = '/%s/%s/sources/%s/concepts/%s/' % (
            country_data_element_owner_type_url_part, self.country_org,
            datimbase.DatimBase.country_source_id, row[DatimImap.IMAP_FIELD_MOH_INDICATOR_ID])
        row['Country To Concept URI'] = '/%s/%s/sources/%s/concepts/%s/' % (
            country_disaggregate_owner_type_url_part, row['Country Disaggregate Owner ID'],
            row['Country Disaggregate Source ID'], moh_disag_id)

        return row

    def has_country_indicator(self, indicator_id='', indicator_name=''):
        """
        Returns whether the specified country indicator is defined in the IMAP
        :param indicator_id:
        :param indicator_name:
        :return: bool
        """
        for row in self.get_imap_data(exclude_empty_maps=True):
            if ((not indicator_id or (indicator_id and indicator_id == row[DatimImap.IMAP_FIELD_MOH_INDICATOR_ID])) and
                    (not indicator_name or (indicator_name and indicator_name == row[DatimImap.IMAP_FIELD_MOH_INDICATOR_NAME]))):
                return True
        return False

    def has_country_disag(self, disag_id='', disag_name=''):
        """
        Returns whether the specified country disag is defined in the IMAP.
        To match a null_disag in the IMAP, set disag_id to the actual null disag value (e.g. "null_disag")
        rather than an empty string or by repeating the country indicator ID.
        Note that empty map rows are ignored.
        :param disag_id:
        :param disag_name:
        :return: bool
        """
        for row in self.get_imap_data(exclude_empty_maps=True):
            if ((not disag_id or (disag_id and disag_id == row[DatimImap.IMAP_FIELD_MOH_DISAG_ID])) and
                    (not disag_name or (disag_name and disag_name == row[DatimImap.IMAP_FIELD_MOH_DISAG_NAME]))):
                return True
        return False

    def has_country_collection(self, csv_row_needle):
        """
        Returns whether the automatically generated collection name from the provided CSV row
        matches one of the automatically generated collection names from a row in this IMAP.
        :param csv_row_needle:
        :return: bool
        """
        # TODO: This method perpetuates the problem! Need to check the actual mappings, not the collection name
        full_csv_row_needle = self.add_columns_to_row(csv_row_needle.copy())
        needle_collection_id = full_csv_row_needle['Country Collection ID']
        if not needle_collection_id:
            return False
        for row in self.get_imap_data(exclude_empty_maps=True, include_extra_info=True):
            if row['Country Collection ID'] == needle_collection_id:
                return True
        return False

    def has_country_operation_mapping(self, csv_row):
        """
        Returns whether a mapping exists in this IMAP that matches the mapping in the provided CSV row.
        A match considers only the DATIM indicator+disag IDs and the MOH indicator+disag IDs.
        Names and the mapping operation (e.g. ADD, SUBTRACT, etc.) are ignored.
        Note that empty mapping rows are ignored.
        :param csv_row:
        :return: bool
        """
        for row in self.get_imap_data(exclude_empty_maps=True):
            if (row[DatimImap.IMAP_FIELD_DATIM_INDICATOR_ID] == csv_row[DatimImap.IMAP_FIELD_DATIM_INDICATOR_ID] and
                    row[DatimImap.IMAP_FIELD_DATIM_DISAG_ID] == csv_row[DatimImap.IMAP_FIELD_DATIM_DISAG_ID] and
                    row[DatimImap.IMAP_FIELD_MOH_INDICATOR_ID] == csv_row[DatimImap.IMAP_FIELD_MOH_INDICATOR_ID] and
                    row[DatimImap.IMAP_FIELD_MOH_DISAG_ID] == csv_row[DatimImap.IMAP_FIELD_MOH_DISAG_ID]):
                return True
        return False

    def has_country_datim_mapping(self, csv_row):
        """
        Returns whether the IMAP contains a mapping for the. Note that empty mapping rows are ignored.
        :param csv_row:
        :return: bool
        """
        for row in self.get_imap_data(exclude_empty_maps=True):
            if (row[DatimImap.IMAP_FIELD_DATIM_INDICATOR_ID] == csv_row[DatimImap.IMAP_FIELD_DATIM_INDICATOR_ID] and
                    row[DatimImap.IMAP_FIELD_DATIM_DISAG_ID] == csv_row[DatimImap.IMAP_FIELD_DATIM_DISAG_ID]):
                return True
        return False

    def get_country_indicator_update_json(self, row):
        if DatimImap.IMAP_EXTRA_FIELD_NAMES[0] not in row:
            row = self.add_columns_to_row(DatimImap.fix_null_disag_in_row(row))
        defs = [DatimMohCsvToJsonConverter.CSV_RESOURCE_DEF_MOH_INDICATOR]
        return DatimImapFactory.generate_import_script_from_csv_row(
            imap_input=self, csv_row=row, defs=defs)

    def get_country_indicator_create_json(self, row):
        if DatimImap.IMAP_EXTRA_FIELD_NAMES[0] not in row:
            row = self.add_columns_to_row(DatimImap.fix_null_disag_in_row(row))
        defs = [DatimMohCsvToJsonConverter.CSV_RESOURCE_DEF_MOH_INDICATOR]
        return DatimImapFactory.generate_import_script_from_csv_row(
            imap_input=self, csv_row=row, defs=defs)

    def get_country_disag_update_json(self, row):
        if DatimImap.IMAP_EXTRA_FIELD_NAMES[0] not in row:
            row = self.add_columns_to_row(DatimImap.fix_null_disag_in_row(row))
        defs = [DatimMohCsvToJsonConverter.CSV_RESOURCE_DEF_MOH_DISAG]
        return DatimImapFactory.generate_import_script_from_csv_row(
            imap_input=self, csv_row=row, defs=defs)

    def get_country_disag_create_json(self, row):
        if DatimImap.IMAP_EXTRA_FIELD_NAMES[0] not in row:
            row = self.add_columns_to_row(DatimImap.fix_null_disag_in_row(row))
        defs = [DatimMohCsvToJsonConverter.CSV_RESOURCE_DEF_MOH_DISAG]
        return DatimImapFactory.generate_import_script_from_csv_row(
            imap_input=self, csv_row=row, defs=defs)

    def get_country_collection_create_json(self, row):
        if DatimImap.IMAP_EXTRA_FIELD_NAMES[0] not in row:
            row = self.add_columns_to_row(DatimImap.fix_null_disag_in_row(row))
        defs = [DatimMohCsvToJsonConverter.CSV_RESOURCE_DEF_MOH_COLLECTION]
        return DatimImapFactory.generate_import_script_from_csv_row(
            imap_input=self, csv_row=row, defs=defs)

    def get_country_operation_mapping_create_json(self, row):
        if DatimImap.IMAP_EXTRA_FIELD_NAMES[0] not in row:
            row = self.add_columns_to_row(DatimImap.fix_null_disag_in_row(row))
        defs = [DatimMohCsvToJsonConverter.CSV_RESOURCE_DEF_MOH_OPERATION_MAPPING]
        return DatimImapFactory.generate_import_script_from_csv_row(
            imap_input=self, csv_row=row, defs=defs)

    def get_country_datim_mapping_create_json(self, row):
        if DatimImap.IMAP_EXTRA_FIELD_NAMES[0] not in row:
            row = self.add_columns_to_row(DatimImap.fix_null_disag_in_row(row))
        defs = [DatimMohCsvToJsonConverter.CSV_RESOURCE_DEF_MOH_DATIM_MAPPING]
        return DatimImapFactory.generate_import_script_from_csv_row(
            imap_input=self, csv_row=row, defs=defs)

    def get_country_operation_mapping_retire_json(self, row):
        if DatimImap.IMAP_EXTRA_FIELD_NAMES[0] not in row:
            row = self.add_columns_to_row(DatimImap.fix_null_disag_in_row(row))
        defs = [DatimMohCsvToJsonConverter.CSV_RESOURCE_DEF_MOH_OPERATION_MAPPING_RETIRED]
        return DatimImapFactory.generate_import_script_from_csv_row(
            imap_input=self, csv_row=row, defs=defs)


class DatimImapFactory(object):
    """
    Factory class for the DatimImap object
    """

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
    def delete_org_if_exists(org_id, oclenv='', ocl_root_api_token=''):
        """
        Delete the org if it exists. Requires a root API token.
        :param org_id:
        :param ocl_root_api_token:
        :return:
        """

        # Check if org exists
        oclapiheaders = {
            'Authorization': 'Token ' + ocl_root_api_token,
            'Content-Type': 'application/json'
        }
        org_url = "%s/orgs/%s/" % (oclenv, org_id)
        r = requests.get(org_url, headers=oclapiheaders)
        if r.status_code != 200:
            return False

        # Delete the org
        r = requests.delete(org_url, headers=oclapiheaders)
        r.raise_for_status()
        print(r)
        return True


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
        """
        Load IMAP from CSV file
        :param csv_filename:
        :param country_code:
        :param country_org:
        :param country_name:
        :param period:
        :return:
        """
        with open(csv_filename, 'rb') as input_file:
            imap_data = csv.DictReader(input_file)
            return DatimImap(imap_data=imap_data, country_code=country_code, country_name=country_name,
                             country_org=country_org, period=period)

    @staticmethod
    def load_imap_from_ocl(oclenv='', oclapitoken='', run_ocl_offline=False,
                           country_code='', country_org='', period='', verbosity=0):
        """
        Fetch an IMAP from OCL. Returns none if country code/org is unrecognized
        :param oclenv:
        :param oclapitoken:
        :param run_ocl_offline:
        :param country_code:
        :param country_org:
        :param period:
        :param verbosity:
        :return:
        """
        datim_imap_export = datimimapexport.DatimImapExport(
            oclenv=oclenv, oclapitoken=oclapitoken, run_ocl_offline=run_ocl_offline, verbosity=verbosity)
        return datim_imap_export.get_imap(
            period=period, country_org=country_org, country_code=country_code)

    @staticmethod
    def create_repo_version(oclenv='', oclapitoken='', repo_endpoint='', repo_version_id=''):
        """
        Create a new repository version
        :param oclenv:
        :param oclapitoken:
        :param repo_endpoint:
        :param repo_version_id:
        :return:
        """
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
        """
        Return a list of JSON imports representing the diff
        :param imap_diff: IMAP diff used to generate the import script
        :return list: Ordered list of dictionaries ready for import
        """
        import_list = []
        import_list_narrative = []
        diff_data = imap_diff.get_diff()

        # Handle 'dictionary_item_added' - new country mapping
        if 'dictionary_item_added' in diff_data:
            for diff_key in diff_data['dictionary_item_added'].keys():
                row_key = diff_key.strip("root['").strip("']")
                csv_row = diff_data['dictionary_item_added'][diff_key]

                # country indicator
                country_indicator_id = csv_row[DatimImap.IMAP_FIELD_MOH_INDICATOR_ID]
                country_indicator_name = csv_row[DatimImap.IMAP_FIELD_MOH_INDICATOR_NAME]
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
                country_disag_id = csv_row[DatimImap.IMAP_FIELD_MOH_DISAG_ID]
                country_disag_name = csv_row[DatimImap.IMAP_FIELD_MOH_DISAG_NAME]
                if imap_diff.imap_a.has_country_disag(
                        disag_id=country_disag_id, disag_name=country_disag_name):
                    # do nothing - disag already exists
                    pass
                elif country_disag_id == datimbase.DatimBase.NULL_DISAG_ID:
                    # do nothing - we do not need to re-create the null_disag concept
                    pass
                elif imap_diff.imap_a.has_country_disag(disag_id=country_disag_id):
                    # update - country disag exists, but name is different
                    import_list_narrative.append('Update country disag: %s, %s' % (
                        country_disag_id, country_disag_name))
                    import_list += imap_diff.imap_b.get_country_disag_update_json(csv_row)
                else:
                    # new - country disag does not exist, so create it
                    import_list_narrative.append('Create new country disag: %s, %s' % (
                        country_disag_id, country_disag_name))
                    import_list += imap_diff.imap_b.get_country_disag_create_json(csv_row)

                # country collection
                # TODO: Compare this against OCL not the original IMAP - low priority
                if not imap_diff.imap_a.has_country_collection(csv_row):
                    full_csv_row = imap_diff.imap_b.add_columns_to_row(csv_row.copy())
                    import_list_narrative.append('Create country collection: %s' % (
                        full_csv_row['Country Collection ID']))
                    import_list += imap_diff.imap_b.get_country_collection_create_json(csv_row)

                # country DATIM mapping
                # TODO: Compare this against OCL not the original IMAP - low priority
                if not imap_diff.imap_a.has_country_datim_mapping(csv_row):
                    import_list_narrative.append('Create DATIM mapping: %s, %s --> %s --> %s, %s' % (
                        csv_row[DatimImap.IMAP_FIELD_DATIM_INDICATOR_CATEGORY], csv_row[DatimImap.IMAP_FIELD_DATIM_INDICATOR_ID],
                        datimbase.DatimBase.map_type_country_has_option,
                        csv_row[DatimImap.IMAP_FIELD_DATIM_DISAG_ID], csv_row[DatimImap.IMAP_FIELD_DATIM_DISAG_NAME]))
                    import_list += imap_diff.imap_b.get_country_datim_mapping_create_json(csv_row)

                # country operation mapping
                # TODO: Compare this against OCL not the original IMAP - low priority
                if not imap_diff.imap_a.has_country_operation_mapping(csv_row):
                    import_list_narrative.append('Create country mapping: %s, %s --> %s --> %s, %s' % (
                        csv_row[DatimImap.IMAP_FIELD_MOH_INDICATOR_ID], csv_row[DatimImap.IMAP_FIELD_MOH_INDICATOR_NAME], csv_row[DatimImap.IMAP_FIELD_OPERATION],
                        csv_row[DatimImap.IMAP_FIELD_MOH_DISAG_ID], csv_row[DatimImap.IMAP_FIELD_MOH_DISAG_NAME]))
                    import_list += imap_diff.imap_b.get_country_operation_mapping_create_json(csv_row)

        # Handle 'dictionary_item_removed' - removed country mapping
        if 'dictionary_item_removed' in diff_data:
            for diff_key in diff_data['dictionary_item_removed'].keys():
                row_key = diff_key.strip("root['").strip("']")
                csv_row = imap_diff.imap_a.get_imap_row_by_key(row_key)

                # Retire country operation mapping
                if imap_diff.imap_a.has_country_operation_mapping(csv_row):
                    import_list_narrative.append('Retire country mapping: %s, %s --> %s --> %s, %s' % (
                        csv_row[DatimImap.IMAP_FIELD_MOH_INDICATOR_ID], csv_row[DatimImap.IMAP_FIELD_MOH_INDICATOR_NAME], csv_row[DatimImap.IMAP_FIELD_OPERATION],
                        csv_row[DatimImap.IMAP_FIELD_MOH_DISAG_ID], csv_row[DatimImap.IMAP_FIELD_MOH_DISAG_NAME]))
                    import_list += imap_diff.imap_a.get_country_operation_mapping_retire_json(csv_row)

                # TODO: Retire country disag
                """
                -- Ignoring for now, because the compare needs to be against OCL itself, not the IMAP object
                Is country disag used by any mappings that are not in the removed list? 
                If no, retire the country disag
                """
                country_disag_id = csv_row[DatimImap.IMAP_FIELD_MOH_DISAG_ID]
                country_disag_name = csv_row[DatimImap.IMAP_FIELD_MOH_DISAG_ID]
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
                country_indicator_id = csv_row[DatimImap.IMAP_FIELD_MOH_INDICATOR_ID]
                country_indicator_name = csv_row[DatimImap.IMAP_FIELD_MOH_INDICATOR_ID]
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
            regex_pattern = "^root\[\'([a-zA-Z0-9\-_]+,[a-zA-Z0-9\-_]+,[a-zA-Z0-9\-_]+,[a-zA-Z0-9\-_]+,[a-zA-Z0-9\-_]+,[a-zA-Z0-9\-_]+,[a-zA-Z0-9\-_]+)\'\]\[\'(MOH_Disag_Name|MOH_Indicator_Name)\'\]$"
            for diff_key in diff_data['values_changed'].keys():
                regex_result = re.match(regex_pattern, diff_key)
                if not regex_result:
                    continue
                row_key = regex_result.group(1)
                matched_field_name = regex_result.group(2)

                #csv_row_old = imap_diff.imap_a.get_imap_row_by_key(row_key)
                csv_row_new = imap_diff.imap_b.get_imap_row_by_key(row_key)

                # MOH_Indicator_Name
                if matched_field_name == DatimImap.IMAP_FIELD_MOH_INDICATOR_NAME:
                    import_list_narrative.append('Update country indicator name: %s, %s' % (
                        csv_row_new[DatimImap.IMAP_FIELD_MOH_INDICATOR_ID], csv_row_new[DatimImap.IMAP_FIELD_MOH_INDICATOR_NAME]))
                    import_list += imap_diff.imap_b.get_country_indicator_update_json(csv_row_new)

                # MOH_Disag_Name
                if matched_field_name == DatimImap.IMAP_FIELD_MOH_DISAG_NAME:
                    import_list_narrative.append('Update country disag name: %s, %s' % (
                        csv_row_new[DatimImap.IMAP_FIELD_MOH_DISAG_ID], csv_row_new[DatimImap.IMAP_FIELD_MOH_DISAG_NAME]))
                    import_list += imap_diff.imap_b.get_country_disag_update_json(csv_row_new)

        # Dedup the import list without changing order
        import_list_dedup = [i for n, i in enumerate(import_list) if i not in import_list[n + 1:]]
        import_list_narrative_dedup = []
        [import_list_narrative_dedup.append(i) for i in import_list_narrative if not import_list_narrative_dedup.count(i)]

        pprint.pprint(import_list_narrative_dedup)
        return import_list_dedup

    @staticmethod
    def generate_import_script_from_csv_row(imap_input=None, csv_row=None, defs=None, do_add_columns_to_csv=True):
        """
        Return a list of JSON imports representing the CSV row
        :param imap_input:
        :param csv_row:
        :param defs:
        :param do_add_columns_to_csv:
        :return:
        """
        if do_add_columns_to_csv:
            csv_row = imap_input.add_columns_to_row(csv_row.copy())
        datim_csv_converter = DatimMohCsvToJsonConverter(input_list=[csv_row])
        datim_csv_resource_definitions = datim_csv_converter.get_country_csv_resource_definitions(
            country_owner=imap_input.country_org,
            country_owner_type=datimbase.DatimBase.country_owner_type,
            country_source=datimbase.DatimBase.country_source_id,
            datim_map_type=datimbase.DatimBase.map_type_country_has_option,
            defs=defs)
        datim_csv_converter.set_resource_definitions(datim_csv_resource_definitions)
        import_list = datim_csv_converter.process_by_definition()
        # Dedup the import list without changing order using list enumeration
        import_list_dedup = [i for n, i in enumerate(import_list) if i not in import_list[n + 1:]]
        return import_list_dedup

    @staticmethod
    def generate_import_script_from_csv(imap_input):
        """
        Return a list of JSON imports representing the entire CSV
        :param imap_input:
        :return:
        """
        datim_csv_converter = DatimMohCsvToJsonConverter(input_list=imap_input.get_imap_data(include_extra_info=True))
        datim_csv_resource_definitions = datim_csv_converter.get_country_csv_resource_definitions(
            country_owner=imap_input.country_org,
            country_owner_type=datimbase.DatimBase.country_owner_type,
            country_source=datimbase.DatimBase.country_source_id,
            datim_map_type=datimbase.DatimBase.map_type_country_has_option)
        datim_csv_converter.set_resource_definitions(datim_csv_resource_definitions)
        import_list = datim_csv_converter.process_by_definition()
        # Dedup the import list using list enumeration
        import_list_dedup = [i for n, i in enumerate(import_list) if i not in import_list[n + 1:]]
        return import_list_dedup

    @staticmethod
    def is_valid_imap_period(period):
        """
        Returns True if specified period is valid
        :param period:
        :return:
        """
        # TODO: Confirm that the period has been defined in the PEPFAR metadata
        return True


class DatimImapDiff(object):
    """ Object representing the diff between two IMAP objects """

    def __init__(self, imap_a, imap_b, exclude_empty_maps=False):
        self.imap_a = imap_a
        self.imap_b = imap_b
        self.__diff_data = None
        self.diff(imap_a, imap_b, exclude_empty_maps=exclude_empty_maps)

    def diff(self, imap_a, imap_b, exclude_empty_maps=False):
        """
        Evaluates the diff between two DatimImap objects
        :param imap_a:
        :param imap_b:
        :param exclude_empty_maps:
        :return:
        """
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
        """
        Returns the diff results
        :return:
        """
        return self.__diff_data


class DatimMohCsvToJsonConverter(ocldev.oclcsvtojsonconverter.OclCsvToJsonConverter):
    """ Extend to add a custom CSV pre-processor """

    CSV_RESOURCE_DEF_MOH_INDICATOR = 'MOH-Indicator'
    CSV_RESOURCE_DEF_MOH_DISAG = 'MOH-Disaggregate'
    CSV_RESOURCE_DEF_MOH_DATIM_MAPPING = 'MOH-Datim-Mapping'
    CSV_RESOURCE_DEF_MOH_OPERATION_MAPPING = 'MOH-Mapping-Operation'
    CSV_RESOURCE_DEF_MOH_COLLECTION = 'MOH-Mapping-Collection'
    CSV_RESOURCE_DEF_MOH_INDICATOR_RETIRED = 'MOH-Indicator-Retired'
    CSV_RESOURCE_DEF_MOH_DISAG_RETIRED = 'MOH-Disaggregate-Retired'
    CSV_RESOURCE_DEF_MOH_DATIM_MAPPING_RETIRED = 'MOH-Datim-Mapping-Retired'
    CSV_RESOURCE_DEF_MOH_OPERATION_MAPPING_RETIRED = 'MOH-Mapping-Operation-Retired'


    @staticmethod
    def get_country_csv_resource_definitions(country_owner='', country_owner_type='',
                                             country_source='', datim_map_type='', defs=None):
        """
        Returns resource definitions for DATIM IMAP CSV
        :param country_owner:
        :param country_owner_type:
        :param country_source:
        :param datim_map_type:
        :param defs:
        :return:
        """
        csv_resource_definitions = [
            {
                'definition_name': DatimMohCsvToJsonConverter.CSV_RESOURCE_DEF_MOH_INDICATOR,
                'is_active': True,
                'resource_type':'Concept',
                'id_column':DatimImap.IMAP_FIELD_MOH_INDICATOR_ID,
                'skip_if_empty_column':DatimImap.IMAP_FIELD_MOH_INDICATOR_ID,
                ocldev.oclcsvtojsonconverter.OclCsvToJsonConverter.DEF_CORE_FIELDS:[
                    {'resource_field':'concept_class', 'value':'Indicator'},
                    {'resource_field':'datatype', 'value':'Numeric'},
                    {'resource_field':'owner', 'column':'Country Data Element Owner ID'},
                    {'resource_field':'owner_type', 'column':'Country Data Element Owner Type'},
                    {'resource_field':'source', 'column':'Country Data Element Source ID'},
                    {'resource_field':'retired', 'value':False},
                ],
                ocldev.oclcsvtojsonconverter.OclCsvToJsonConverter.DEF_SUB_RESOURCES:{
                    'names':[
                        [
                            {'resource_field':'name', 'column':DatimImap.IMAP_FIELD_MOH_INDICATOR_NAME},
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
                'id_column':DatimImap.IMAP_FIELD_MOH_DISAG_ID,
                'skip_if_empty_column':DatimImap.IMAP_FIELD_MOH_DISAG_ID,
                ocldev.oclcsvtojsonconverter.OclCsvToJsonConverter.DEF_CORE_FIELDS:[
                    {'resource_field':'concept_class', 'value':'Disaggregate'},
                    {'resource_field':'datatype', 'value':'None'},
                    {'resource_field':'owner', 'column':'Country Disaggregate Owner ID'},
                    {'resource_field':'owner_type', 'column':'Country Disaggregate Owner Type'},
                    {'resource_field':'source', 'column':'Country Disaggregate Source ID'},
                    {'resource_field':'retired', 'value':False},
                ],
                ocldev.oclcsvtojsonconverter.OclCsvToJsonConverter.DEF_SUB_RESOURCES:{
                    'names':[
                        [
                            {'resource_field':'name', 'column':DatimImap.IMAP_FIELD_MOH_DISAG_NAME},
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
                'skip_if_empty_column':DatimImap.IMAP_FIELD_MOH_DISAG_ID,
                'internal_external': {'value':ocldev.oclcsvtojsonconverter.OclCsvToJsonConverter.INTERNAL_MAPPING_ID},
                ocldev.oclcsvtojsonconverter.OclCsvToJsonConverter.DEF_CORE_FIELDS:[
                    {'resource_field':'from_concept_url', 'column':'DATIM From Concept URI'},
                    {'resource_field':'map_type', 'value':datim_map_type},
                    {'resource_field':'to_concept_url', 'column':'DATIM To Concept URI'},
                    {'resource_field':'owner', 'value':country_owner},
                    {'resource_field':'owner_type', 'value':country_owner_type},
                    {'resource_field':'source', 'value':country_source},
                    {'resource_field':'retired', 'value':False},
                ]
            },
            {
                'definition_name': DatimMohCsvToJsonConverter.CSV_RESOURCE_DEF_MOH_OPERATION_MAPPING,
                'is_active': True,
                'resource_type': 'Mapping',
                'id_column': None,
                'skip_if_empty_column': DatimImap.IMAP_FIELD_OPERATION,
                'internal_external': {'value':ocldev.oclcsvtojsonconverter.OclCsvToJsonConverter.INTERNAL_MAPPING_ID},
                ocldev.oclcsvtojsonconverter.OclCsvToJsonConverter.DEF_CORE_FIELDS:[
                    {'resource_field':'from_concept_url', 'column':'Country From Concept URI'},
                    {'resource_field':'map_type', 'column':'Country Map Type'},
                    {'resource_field':'to_concept_url', 'column':'Country To Concept URI'},
                    {'resource_field':'owner', 'value':country_owner},
                    {'resource_field':'owner_type', 'value':country_owner_type},
                    {'resource_field':'source', 'value':country_source},
                    {'resource_field':'retired', 'value':False},
                ]
            },
            {
                'definition_name': DatimMohCsvToJsonConverter.CSV_RESOURCE_DEF_MOH_OPERATION_MAPPING_RETIRED,
                'is_active': True,
                'resource_type': 'Mapping',
                'id_column': None,
                'internal_external': {'value': ocldev.oclcsvtojsonconverter.OclCsvToJsonConverter.INTERNAL_MAPPING_ID},
                ocldev.oclcsvtojsonconverter.OclCsvToJsonConverter.DEF_CORE_FIELDS: [
                    {'resource_field': 'from_concept_url', 'column': 'Country From Concept URI'},
                    {'resource_field': 'map_type', 'column': 'Country Map Type'},
                    {'resource_field': 'to_concept_url', 'column': 'Country To Concept URI'},
                    {'resource_field': 'owner', 'value': country_owner},
                    {'resource_field': 'owner_type', 'value': country_owner_type},
                    {'resource_field': 'source', 'value': country_source},
                    {'resource_field': 'retired', 'value': True},
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
