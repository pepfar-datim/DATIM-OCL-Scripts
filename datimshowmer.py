from __future__ import with_statement
import os
import sys
import json
import csv
from xml.etree.ElementTree import Element
from xml.etree.ElementTree import SubElement
from xml.etree.ElementTree import tostring
from datimshow import DatimShow
from datimbase import DatimConstants


class DatimShowMer(DatimShow):
    """ Class to manage DATIM MER Presentation """

    # OCL Export Definitions
    OCL_EXPORT_DEFS = DatimConstants.MER_OCL_EXPORT_DEFS

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

    def get(self, collection_id='', export_format=None):
        """
        Output the specified collection the requested format
        :param collection_id: OCL Collection ID to present
        :param export_format: Format to present the data in
        :return: None
        """

        # Validate export_format
        if export_format not in self.PRESENTATION_FORMATS:
            export_format = self.DATIM_FORMAT_HTML

        # Get the export definition
        collection_title = ''
        collection_subtitle = ''
        if collection_id in self.OCL_EXPORT_DEFS:
            collection_endpoint = self.OCL_EXPORT_DEFS[collection_id]['endpoint']
            if 'title' in self.OCL_EXPORT_DEFS[collection_id]:
                collection_title = self.OCL_EXPORT_DEFS[collection_id]['title']
            if 'subtitle' in self.OCL_EXPORT_DEFS[collection_id]:
                collection_subtitle = self.OCL_EXPORT_DEFS[collection_id]['subtitle']
        else:
            collection_endpoint = '/orgs/PEPFAR/collections/%s/' % collection_id
        if not collection_title:
            collection_title = collection_id

        # STEP 1. Fetch latest version of relevant OCL repository export
        self.vlog(1, '**** STEP 1: Fetch latest version of relevant OCL repository export')
        self.vlog(1, '%s:' % collection_endpoint)
        tarfilename = self.endpoint2filename_ocl_export_tar(collection_endpoint)
        jsonfilename = self.endpoint2filename_ocl_export_json(collection_endpoint)
        if not self.run_ocl_offline:
            self.get_ocl_export(endpoint=collection_endpoint, version='latest', tarfilename=tarfilename,
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
        jsonfilename = self.endpoint2filename_ocl_export_json(collection_endpoint)
        intermediate_json_filename = self.endpoint2filename_ocl_export_intermediate_json(collection_endpoint)
        with open(self.attach_absolute_path(jsonfilename), 'rb') as ifile, open(
                self.attach_absolute_path(intermediate_json_filename), 'wb') as ofile:
            ocl_export_raw = json.load(ifile)
            intermediate = {
                'title': collection_title,
                'subtitle': collection_subtitle,
                'width': 4,
                'height': 0,
                'headers': [
                    {"name": "dataset", "column": "dataset", "type": "java.lang.String", "hidden": False, "meta": False},
                    {"name": "dataelement", "column": "dataelement", "type": "java.lang.String", "hidden": False, "meta": False},
                    {"name": "shortname", "column": "shortname", "type": "java.lang.String", "hidden": False, "meta": False},
                    {"name": "code", "column": "code", "type": "java.lang.String", "hidden": False, "meta": False},
                    {"name": "dataelementuid", "column": "dataelementuid", "type": "java.lang.String", "hidden": False, "meta": False},
                    {"name": "dataelementdesc", "column": "dataelementdesc","type": "java.lang.String", "hidden": False, "meta": False},
                    {"name": "categoryoptioncombo", "column": "categoryoptioncombo", "type": "java.lang.String", "hidden": False, "meta": False},
                    {"name": "categoryoptioncombocode", "column": "categoryoptioncombocode", "type": "java.lang.String", "hidden": False, "meta": False},
                    {"name": "categoryoptioncombouid", "column": "categoryoptioncombouid", "type": "java.lang.String", "hidden": False, "meta": False}
                ],
                'rows': []
            }

            # Iterate through concepts, clean, then write
            for c in ocl_export_raw['concepts']:
                if c['concept_class'] == 'Indicator':
                    # Build the indicator
                    concept_description = ''
                    if c['descriptions']:
                        concept_description = c['descriptions'][0]['description']
                    output_concept = {
                        'dataset': collection_title.encode('utf-8'),
                        'dataelement': c['names'][0]['name'].encode('utf-8'),
                        'shortname': c['names'][1]['name'].encode('utf-8'),
                        'code': c['id'].encode('utf-8'),
                        'dataelementuid': c['external_id'].encode('utf-8'),
                        'dataelementdesc': concept_description.encode('utf-8'),
                        'categoryoptioncombo': '',
                        'categoryoptioncombocode': '',
                        'categoryoptioncombouid': '',
                    }

                    # Find all the relevant mappings
                    mappings = [item for item in ocl_export_raw['mappings'] if str(
                        item["from_concept_url"]) == c['url']]
                    if mappings:
                        for m in mappings:
                            output_concept['categoryoptioncombo'] = m['to_concept_name'].encode('utf-8')
                            output_concept['categoryoptioncombocode'] = m['to_concept_code'].encode('utf-8')
                            output_concept['categoryoptioncombouid'] = m['to_concept_code'].encode('utf-8')
                            intermediate['rows'].append(output_concept.copy())
                    if not mappings:
                        intermediate['rows'].append(output_concept.copy())
            intermediate['height'] = len(intermediate['rows'])

            # Write intermediate state as a file (for future caching)
            ofile.write(json.dumps(intermediate))
            self.vlog(1, 'Processed OCL export saved to "%s"' % intermediate_json_filename)

        # STEP 3: Transform to requested format and stream
        self.vlog(1, '**** STEP 3: Transform to requested format and stream')
        if export_format == self.DATIM_FORMAT_HTML:
            self.transform_to_html(intermediate)
        elif export_format == self.DATIM_FORMAT_JSON:
            self.transform_to_json(intermediate)
        elif export_format == self.DATIM_FORMAT_XML:
            self.transform_to_xml(intermediate)
        elif export_format == self.DATIM_FORMAT_CSV:
            self.transform_to_csv(intermediate)

    def transform_to_html(self, content):
        css = ('<style tye="text/css">.gridDiv {font-family:sans-serif, arial;}'
               'table.gridTable {border-collapse: collapse; font-size: 11pt;}'
               '.gridTable th,.gridTable td {padding: 8px 4px 7px 4px; border: 1px solid #e7e7e7;}'
               '.gridTable th {background-color: #f3f3f3; font-weight: bold;}</style>\n')
        sys.stdout.write(css)
        sys.stdout.write('<div class="gridDiv"><h3>%s</h3>\n' % content['title'])
        sys.stdout.write('<h4>%s</h4>\n' % content['subtitle'])
        sys.stdout.write('<table class="gridTable">\n<thead><tr>')
        for h in content['headers']:
            sys.stdout.write('<th>%s</th>' % str(h['name']))
        sys.stdout.write('</tr></thead>\n<tbody>')
        for row in content['rows']:
            sys.stdout.write('\n<tr>')
            for h in content['headers']:
                sys.stdout.write('<td>%s</td>' % str(row[h['name']]))
        sys.stdout.write('</tr>')
        sys.stdout.write('\n</tbody></table></div>')
        sys.stdout.flush()

    def transform_to_json(self, content):
        sys.stdout.write(json.dumps(content, indent=4))
        sys.stdout.flush()

    def xml_dict_clean(self, intermediate_data):
        new_dict = {}
        for k, v in intermediate_data.iteritems():
            if isinstance(v, bool):
                if v:
                    v = "true"
                else:
                    v = "false"
            new_dict[k] = str(v)
        return new_dict

    def transform_to_xml(self, content):
        top_attr = {
            'title': content['title'],
            'subtitle': content['subtitle'],
            'width': str(content['width']),
            'height': str(content['height'])
        }
        top = Element('grid', top_attr)
        headers = SubElement(top, 'headers')
        for h in content['headers']:
            SubElement(headers, 'header', self.xml_dict_clean(h))
        rows = SubElement(top, 'rows')
        for row_values in content['rows']:
            row = SubElement(rows, 'row')
            for field_name in row_values:
                field = SubElement(row, 'field')
                field.text = row_values[field_name].encode('utf-8')
        print(tostring(top))

    def transform_to_csv(self, content):
        fieldnames = []
        for h in content['headers']:
            fieldnames.append(h['name'])
        w = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
        w.writeheader()
        for row in content['rows']:
            w.writerow(row)
        sys.stdout.flush()


# Default Script Settings
verbosity = 0  # 0=none, 1=some, 2=all
run_ocl_offline = False  # Set to true to use local copies of dhis2/ocl exports

# Set some defaults
export_format = DatimShow.DATIM_FORMAT_XML
collection_id = 'MER-R-Operating-Unit-Level-IM-FY17Q2'

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

    # Requested Collection
    collection_id = sys.argv[2]

# OCL Settings
#oclenv = ''
#oclapitoken = ''

# JetStream Staging user=datim-admin
oclenv = 'https://api.staging.openconceptlab.org'
oclapitoken = 'c3b42623c04c87e266d12ae0e297abbce7f1cbe8'

# Create Show object and run
datim_show = DatimShowMer(oclenv=oclenv, oclapitoken=oclapitoken, run_ocl_offline=run_ocl_offline, verbosity=verbosity)
datim_show.get(collection_id=collection_id, export_format=export_format)
