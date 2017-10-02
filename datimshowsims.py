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


class DatimShowSims(DatimShow):
    """ Class to manage DATIM SIMS Presentation """

    # OCL Export Definitions
    OCL_EXPORT_DEFS = {
        'sims_source': {
            'endpoint': '/orgs/PEPFAR/sources/SIMS/',
            'tarfilename': 'ocl_sims_source_export.tar',
            'jsonfilename': 'ocl_sims_source_export_raw.json',
            'intermediatejsonfilename': 'ocl_sims_source_export_intermediate.json',
        }
    }

    def __init__(self, oclenv='', oclapitoken='',
                 run_ocl_offline=False, verbosity=0):
        self.oclenv = oclenv
        self.oclapitoken = oclapitoken
        self.run_ocl_offline = run_ocl_offline
        self.verbosity = verbosity

    def get(self, export_format):
        """

        :param export_format:
        :return:
        """
        # STEP 1. Fetch latest versions of relevant OCL exports
        self.vlog(1, '**** STEP 1: Fetch latest versions of relevant OCL exports')
        for ocl_export_def_key in self.OCL_EXPORT_DEFS:
            self.log(1, '%s:' % ocl_export_def_key)
            export_def = self.OCL_EXPORT_DEFS[ocl_export_def_key]
            if not self.run_ocl_offline:
                self.get_ocl_export(
                    endpoint=export_def['endpoint'],
                    version='latest',
                    tarfilename=export_def['tarfilename'],
                    jsonfilename=export_def['jsonfilename'])
            else:
                self.vlog(1, 'OCL-OFFLINE: Using local file "%s"...' % (export_def['jsonfilename']))
                if os.path.isfile(self.attach_absolute_path(export_def['jsonfilename'])):
                    self.vlog(1, 'OCL-OFFLINE: File "%s" found containing %s bytes. Continuing...' % (
                        export_def['jsonfilename'], os.path.getsize(self.attach_absolute_path(export_def['jsonfilename']))))
                else:
                    self.log('Could not find offline OCL file "%s". Exiting...' % (export_def['jsonfilename']))
                    sys.exit(1)

        # STEP 2: Transform OCL export to intermediary state
        self.vlog(1, '**** STEP 2: Transform to intermediary state')
        sims_intermediate = {}
        for ocl_export_def_key in self.OCL_EXPORT_DEFS:
            self.vlog(1, '%s:' % ocl_export_def_key)
            export_def = self.OCL_EXPORT_DEFS[ocl_export_def_key]
            with open(self.attach_absolute_path(export_def['jsonfilename']), 'rb') as ifile, open(
                    self.attach_absolute_path(export_def['intermediatejsonfilename']), 'wb') as ofile:
                ocl_sims_export = json.load(ifile)
                sims_intermediate = {
                    'title': 'SIMS v3: Facility Based Data Elements',
                    'subtitle': 'This view shows the name and code for data elements belonging to the SIMS v3 '
                                'Data Element Group (UID = FZxMe3kfzYo)',
                    'width': 4,
                    'height': 0,
                    'headers': [
                        {"name": "name", "column": "name", "type": "java.lang.String", "hidden": False, "meta": False},
                        {"name": "code", "column": "code", "type": "java.lang.String", "hidden": False, "meta": False},
                        {"name": "uid", "column": "uid", "type": "java.lang.String", "hidden": False, "meta": False},
                        {"name": "valuetype", "column": "valuetype", "type": "java.lang.String", "hidden": False, "meta": False}
                    ],
                    'rows': []
                }

                # Iterate through concepts, clean, then write
                for c in ocl_sims_export['concepts']:
                    sims_intermediate['rows'].append({
                        'name': c['names'][0]['name'],
                        'code': c['id'],
                        'uid': c['external_id'],
                        'valuetype': c['extras']['Value Type']
                    })
                sims_intermediate['height'] = len(sims_intermediate['rows'])

                # Write intermediate state as a file (for future caching)
                ofile.write(json.dumps(sims_intermediate))
                self.vlog(1, 'Processed OCL export saved to "%s"' % (export_def['intermediatejsonfilename']))

        # STEP 3: Transform to requested format and stream
        self.vlog(1, '**** STEP 3: Transform to requested format and stream')
        if export_format == self.DATIM_FORMAT_HTML:
            self.transform_to_html(sims_intermediate)
        elif export_format == self.DATIM_FORMAT_JSON:
            self.transform_to_json(sims_intermediate)
        elif export_format == self.DATIM_FORMAT_XML:
            self.transform_to_xml(sims_intermediate)
        elif export_format == self.DATIM_FORMAT_CSV:
            self.transform_to_csv(sims_intermediate)
        else:
            pass

    def transform_to_html(self, sims):
        sys.stdout.write('<div><h3>' + sims['title'] + '</h3>\n')
        sys.stdout.write('<h4>' + sims['subtitle'] + '</h4>\n')
        sys.stdout.write('<table>\n<thead><tr>')
        for h in sims['headers']:
            sys.stdout.write('<th>' + h['name'] + '</th>')
        sys.stdout.write('</tr></thead>\n<tbody>')
        for row in sims['rows']:
            sys.stdout.write('\n<tr>')
            for h in sims['headers']:
                sys.stdout.write('<td>' + row[h['name']] + '</td>')
        sys.stdout.write('</tr>')
        sys.stdout.write('\n</tbody></table></div>')
        sys.stdout.flush()

    def transform_to_json(self, sims):
        sys.stdout.write(json.dumps(sims, indent=4))
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

    def transform_to_xml(self, sims):
        top_attr = {
            'title': sims['title'],
            'subtitle': sims['subtitle'],
            'width': str(sims['width']),
            'height': str(sims['height'])
        }
        top = Element('grid', top_attr)
        headers = SubElement(top, 'headers')
        for h in sims['headers']:
            SubElement(headers, 'header', self.xml_dict_clean(h))
        rows = SubElement(top, 'rows')
        for row_values in sims['rows']:
            row = SubElement(rows, 'row')
            for value in row_values:
                field = SubElement(row, 'field')
                field.text = value
        print(tostring(top))

    def transform_to_csv(self, sims):
        fieldnames = []
        for h in sims['headers']:
            fieldnames.append(h['name'])
        w = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
        w.writeheader()
        for row in sims['rows']:
            w.writerow(row)
        sys.stdout.flush()


# Default Script Settings
verbosity = 0  # 0=none, 1=some, 2=all
run_ocl_offline = False  # Set to true to use local copies of dhis2/ocl exports

# Export Format - see constants in DatimShowSims class
export_format = DatimShowSims.DATIM_FORMAT_JSON

# Requested Collection
collection = ''

# OCL Settings
#oclenv = ''
#oclapitoken = ''

# Local configuration
oclenv = 'https://api.showcase.openconceptlab.org'
oclapitoken = '2da0f46b7d29aa57970c0b3a535121e8e479f881'

# Create SIMS Show object and run
# TODO: Add parameter to specify which collection
sims_show = DatimShowSims(oclenv=oclenv, oclapitoken=oclapitoken,
                          run_ocl_offline=run_ocl_offline, verbosity=verbosity)
sims_show.get(export_format=export_format)
