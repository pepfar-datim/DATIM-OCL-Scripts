import csv
import sys
import json
from xml.etree.ElementTree import Element
from xml.etree.ElementTree import SubElement
from xml.etree.ElementTree import tostring
from datimbase import DatimBase


class DatimShow(DatimBase):

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

    def __init__(self):
        DatimBase.__init__(self)

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
        css = ('<style tye="text/css">.gridDiv {font-family:sans-serif, arial;}'
               'table.gridTable {border-collapse: collapse; font-size: 11pt;}'
               '.gridTable th,.gridTable td {padding: 8px 4px 7px 4px; border: 1px solid #e7e7e7;}'
               '.gridTable th {background-color: #f3f3f3; font-weight: bold;}</style>\n')
        sys.stdout.write(css)
        sys.stdout.write('<div class="gridDiv"><h3>%s</h3>\n' % content['title'].encode('utf-8'))
        if 'subtitle' in content and content['subtitle']:
            sys.stdout.write('<h4>%s</h4>\n' % content['subtitle'].encode('utf-8'))
        sys.stdout.write('<table class="gridTable">\n<thead><tr>')
        for h in content['headers']:
            sys.stdout.write('<th>%s</th>' % str(h['name'].encode('utf-8')))
        sys.stdout.write('</tr></thead>\n<tbody>')
        for row in content['rows']:
            sys.stdout.write('\n<tr>')
            for h in content['headers']:
                sys.stdout.write('<td>%s</td>' % str(row[h['name']].encode('utf-8')))
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
                field.text = row_values[field_name]
        print(tostring(top))

    def transform_to_csv(self, content):
        fieldnames = []
        for h in content['headers']:
            fieldnames.append(h['name'])
        w = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
        w.writeheader()
        for row in content['rows']:
            # convert to utf-8 encoded strings
            row_utf8 = {}
            for k in row:
                row_utf8[k] = row[k].encode('utf-8')
            w.writerow(row_utf8)
        sys.stdout.flush()
