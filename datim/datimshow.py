"""
Shared class for custom presentations (i.e. shows) of DATIM metadata
"""
import csv
import sys
import json
from xml.etree.ElementTree import Element
from xml.etree.ElementTree import SubElement
from xml.etree.ElementTree import tostring
import datimbase


class DatimShow(datimbase.DatimBase):
    """
    Shared class for custom presentations (i.e. shows) of DATIM metadata
    """

    # Presentation Formats
    DATIM_FORMAT_HTML = 'html'
    DATIM_FORMAT_XML = 'xml'
    DATIM_FORMAT_JSON = 'json'
    DATIM_FORMAT_CSV = 'csv'
    PRESENTATION_FORMATS = [
        DATIM_FORMAT_HTML,
        DATIM_FORMAT_XML,
        DATIM_FORMAT_JSON,
        DATIM_FORMAT_CSV
    ]

    # Set to True to only allow presentation of OCL repositories explicitly
    # defined in OCL_EXPORT_DEFS
    REQUIRE_OCL_EXPORT_DEFINITION = False

    # Set the default presentation row building method
    DEFAULT_SHOW_BUILD_ROW_METHOD = 'default_show_build_row'

    # Default endpoint to use if unspecified OCL export
    DEFAULT_REPO_LIST_ENDPOINT = ''

    def __init__(self):
        datimbase.DatimBase.__init__(self)
        self.run_ocl_offline = False
        self.cache_intermediate = True

    def build_show_grid(self, repo_title='', repo_subtitle='', headers='',
                        concepts_with_mappings=None, input_filename='',
                        show_build_row_method=''):
        """ Builds the intermediate export from source data """
        # Setup the headers
        intermediate = {
            'title': repo_title,
            'subtitle': repo_subtitle,
            'height': 0,
            'headers': headers,
            'rows': []
        }
        intermediate['width'] = len(intermediate['headers'])

        # Read in the content from the file saved to disk
        if input_filename:
            with open(self.attach_absolute_data_path(input_filename), 'rb') as input_file:
                ocl_export_raw = json.load(input_file)
                raw_concepts_dict = {ocl_export_raw['concepts'][i]['url']: ocl_export_raw[
                    'concepts'][i] for i in range(len(ocl_export_raw['concepts']))}
                raw_mappings = ocl_export_raw['mappings']

                # add the to_concepts to the mappings so that they are available to the
                # "show_build_row_method" method
                for i in range(len(raw_mappings)):
                    to_concept_url = raw_mappings[i]['to_concept_url']
                    if to_concept_url in raw_concepts_dict:
                        raw_mappings[i]['to_concept'] = raw_concepts_dict[to_concept_url]

                # Add the mappings to the from concepts
                for concept_id in raw_concepts_dict:
                    concept = raw_concepts_dict[concept_id]
                    concept['mappings'] = [mapping for mapping in raw_mappings if str(
                        mapping["from_concept_url"]) == concept['url']]

                concepts_with_mappings = raw_concepts_dict.values()

        elif isinstance(concepts_with_mappings, list):
            # These are already in the correct format
            pass
        else:
            raise Exception('Must provide either "input_filename" or "concepts_with_mappings".')

        if concepts_with_mappings:
            for concept in concepts_with_mappings:
                result = getattr(self, show_build_row_method)(
                    concept, headers=headers, direct_mappings=concept['mappings'],
                    repo_title=repo_title, repo_subtitle=repo_subtitle)
                if result:
                    if isinstance(result, dict):
                        intermediate['rows'].append(result)
                    elif isinstance(result, list):
                        for item in result:
                            intermediate['rows'].append(item)
            intermediate['height'] = len(intermediate['rows'])
        return intermediate

    def default_show_build_row(self, concept, headers=None, direct_mappings=None,
                               repo_title='', repo_subtitle=''):
        """ Default method for building one output row in the presentation layer """
        row = {}
        for header in headers:
            row[header['column']] = ''
        row[headers[0]['column']] = str(concept)
        return row

    def transform_to_format(self, content, export_format):
        """
        Displays the intermediate content in the requested export format
        :param content: Intermediate content shared by all formats
        :param export_format: Export format
        :return:
        """
        if export_format == self.DATIM_FORMAT_HTML:
            self.transform_to_html(content)
        elif export_format == self.DATIM_FORMAT_JSON:
            self.transform_to_json(content)
        elif export_format == self.DATIM_FORMAT_XML:
            self.transform_to_xml(content)
        elif export_format == self.DATIM_FORMAT_CSV:
            self.transform_to_csv(content)

    def transform_to_html(self, content):
        """ Transform intermediate export to HTML """
        css = ('<style tye="text/css">.gridDiv {font-family:sans-serif, arial;}'
               'table.gridTable {border-collapse: collapse; font-size: 11pt;}'
               '.gridTable th,.gridTable td {padding: 8px 4px 7px 4px; border: 1px solid #e7e7e7;}'
               '.gridTable th {background-color: #f3f3f3; font-weight: bold;}</style>\n')
        sys.stdout.write(css)
        sys.stdout.write('<div class="gridDiv"><h3>%s</h3>\n' % content['title'].encode('utf-8'))
        if 'subtitle' in content and content['subtitle']:
            sys.stdout.write('<h4>%s</h4>\n' % content['subtitle'].encode('utf-8'))
        sys.stdout.write('<table class="gridTable">\n<thead><tr>')
        for header in content['headers']:
            sys.stdout.write('<th>%s</th>' % str(header['name']))
        sys.stdout.write('</tr></thead>\n<tbody>')
        for row in content['rows']:
            sys.stdout.write('\n<tr>')
            for header in content['headers']:
                try:
                    sys.stdout.write('<td>%s</td>' % str(row[header['name']].encode('utf-8')))
                except:
                    sys.stdout.write('<td>%s</td>' % str(row[header['name']]))
        sys.stdout.write('</tr>')
        sys.stdout.write('\n</tbody></table></div>')
        sys.stdout.flush()

    def transform_to_json(self, content):
        """ Transform intermediate export to JSON """
        # convert the rows to lists in the same column order as the headers
        reduced_rows = []
        for row in content['rows']:
            reduced_row = []
            for header in content['headers']:
                reduced_row.append(row[header['name']])
            reduced_rows.append(reduced_row)
        content['rows'] = reduced_rows
        sys.stdout.write(json.dumps(content, indent=4, sort_keys=True))
        sys.stdout.flush()

    def xml_dict_clean(self, intermediate_data):
        """ Cleans data for XML export """
        new_dict = {}
        for key, value in intermediate_data.iteritems():
            if isinstance(value, bool):
                if value:
                    value = "true"
                else:
                    value = "false"
            new_dict[key] = str(value)
        return new_dict

    def transform_to_xml(self, content):
        """ Transform intermediate export to XML """
        top_attr = {
            'title': content['title'],
            'subtitle': content['subtitle'],
            'width': str(content['width']),
            'height': str(content['height'])
        }
        top = Element('grid', top_attr)
        headers = SubElement(top, 'headers')
        for header in content['headers']:
            SubElement(headers, 'header', self.xml_dict_clean(header))
        rows = SubElement(top, 'rows')
        for row_values in content['rows']:
            row = SubElement(rows, 'row')
            for field_name in row_values:
                field = SubElement(row, 'field')
                field.text = row_values[field_name]
        print tostring(top)

    def transform_to_csv(self, content):
        """ Transform intermediate export to CSV """
        fieldnames = []
        for header in content['headers']:
            fieldnames.append(header['name'])
        writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
        writer.writeheader()
        for row in content['rows']:
            # convert to utf-8 encoded strings
            row_utf8 = {}
            for key in row:
                try:
                    row_utf8[key] = row[key].encode('utf-8')
                except:
                    row_utf8[key] = row[key]
            writer.writerow(row_utf8)
        sys.stdout.flush()

    @staticmethod
    def get_format_from_string(format_string, default_fmt='html'):
        """ Return DATIM Presentation format constant that matches a string """
        for fmt in DatimShow.PRESENTATION_FORMATS:
            if format_string.lower() == fmt.lower():
                return fmt
        return default_fmt

    def get(self, repo_id='', export_format=''):
        """
        Get the repository in the specified format
        :param repo_id: ID of the repo that matches an OCL_EXPORT_DEF key
        :param export_format: One of the supported export formats. See DATIM_FORMAT constants
        :return:
        """
        repo_title = ''
        repo_subtitle = ''
        repo_endpoint = ''
        show_build_row_method = ''
        show_headers_key = ''
        if export_format not in self.PRESENTATION_FORMATS:
            export_format = self.DATIM_FORMAT_HTML

        # Setup the export
        if repo_id in self.OCL_EXPORT_DEFS:
            repo_endpoint = self.OCL_EXPORT_DEFS[repo_id]['endpoint']
            repo_title = self.OCL_EXPORT_DEFS[repo_id].get('title')
            repo_subtitle = self.OCL_EXPORT_DEFS[repo_id].get('subtitle', '')
            show_build_row_method = self.OCL_EXPORT_DEFS[repo_id].get('show_build_row_method', '')
            show_headers_key = self.OCL_EXPORT_DEFS[repo_id].get('show_headers_key', '')
        elif not self.REQUIRE_OCL_EXPORT_DEFINITION:
            repo_endpoint = '%s%s/' % (self.DEFAULT_REPO_LIST_ENDPOINT, repo_id)
            show_build_row_method = self.DEFAULT_SHOW_BUILD_ROW_METHOD
        else:
            msg = 'Unrecognized key "%s"' % repo_id
            self.log(msg)
            raise Exception(msg)
        if not repo_title:
            repo_title = repo_id
        if not show_headers_key:
            show_headers_key = self.headers.items()[0][0]

        # STEP 1 of 4: Fetch latest version of relevant OCL repository export
        self.vlog(1, '**** STEP 1 of 4: Fetch latest version of relevant OCL repository export')
        self.vlog(1, '%s:' % repo_endpoint)
        zip_filename = self.endpoint2filename_ocl_export_zip(repo_endpoint)
        json_filename = self.endpoint2filename_ocl_export_json(repo_endpoint)
        if not self.run_ocl_offline:
            self.get_ocl_export(endpoint=repo_endpoint, version='latest', zipfilename=zip_filename,
                                jsonfilename=json_filename)
        else:
            self.does_offline_data_file_exist(json_filename, exit_if_missing=True)

        # STEP 2 of 4: Transform OCL export to intermediary state
        self.vlog(1, '**** STEP 2 of 4: Transform to intermediary state')
        json_filename = self.endpoint2filename_ocl_export_json(repo_endpoint)
        intermediate = self.build_show_grid(
            repo_title=repo_title, repo_subtitle=repo_subtitle,
            headers=self.headers[show_headers_key], input_filename=json_filename,
            show_build_row_method=show_build_row_method)

        # STEP 3 of 4: Cache the intermediate output
        self.vlog(1, '**** STEP 3 of 4: Cache the intermediate output')
        if self.cache_intermediate:
            filename = self.endpoint2filename_ocl_export_intermediate_json(repo_endpoint)
            with open(self.attach_absolute_data_path(filename), 'wb') as output_file:
                output_file.write(json.dumps(intermediate))
                self.vlog(1, 'Processed OCL export saved to "%s"' % filename)
        else:
            self.vlog(1, 'SKIPPING: "cache_intermediate" set to "false"')

        # STEP 4 of 4: Transform to requested format and stream
        self.vlog(1, '**** STEP 4 of 4: Transform to requested format and stream')
        self.transform_to_format(intermediate, export_format)

    def getByDataElementIds(self, data_element_ids='', export_format=''):
        """
        Change "get" to a router function that routes the request to getByDataElementIds or getByExport
        Get the a repository in the specified format
        :param data_element_ids: ID of the datim element id's
        :param export_format: One of the supported export formats. See DATIM_FORMAT constants
        :return:
        """
        return None
