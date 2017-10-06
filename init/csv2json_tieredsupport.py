"""
Script to convert Collections CSV file to OCL-formatted JSON
"""
from csv_to_json_flex import ocl_csv_to_json_flex

csv_filename_dataelements = 'Tiered-Support-Data-Elements.csv'
csv_filename_options = 'Tiered-Support-Options.csv'
output_filename = 'tiered_support.json'

csv_resource_defs_dataelements = [
    {
        'definition_name': 'DATIM-TieredSupportDataElements',
        'is_active': True,
        'resource_type': 'Concept',
        'id_column': 'code',
        'skip_if_empty_column': 'code',
        ocl_csv_to_json_flex.DEF_CORE_FIELDS: [
            {'resource_field': 'owner', 'value': 'PEPFAR'},
            {'resource_field': 'owner_type', 'value': 'Organization'},
            {'resource_field': 'source', 'value': 'Tiered-Site-Support'},
            {'resource_field': 'concept_class', 'value': 'Misc'},
            {'resource_field': 'datatype', 'value': 'None'},
            {'resource_field': 'external_id', 'column': 'uid'},
        ],
        ocl_csv_to_json_flex.DEF_SUB_RESOURCES: {
            'names': [
                [
                    {'resource_field': 'name', 'column': 'name'},
                    {'resource_field': 'locale', 'value': 'en'},
                    {'resource_field': 'locale_preferred', 'value': 'True'},
                    {'resource_field': 'name_type', 'value': 'Fully Specified'},
                ],
            ],
        },
    }
]
csv_resource_defs_options = [
    {
        'definition_name': 'DATIM-TieredSupportOptions',
        'is_active': True,
        'resource_type': 'Concept',
        'id_column': 'id',
        'skip_if_empty_column': 'id',
        ocl_csv_to_json_flex.DEF_CORE_FIELDS: [
            {'resource_field': 'owner', 'value': 'PEPFAR'},
            {'resource_field': 'owner_type', 'value': 'Organization'},
            {'resource_field': 'source', 'value': 'Tiered-Site-Support'},
            {'resource_field': 'concept_class', 'value': 'Option'},
            {'resource_field': 'datatype', 'value': 'None'},
        ],
        ocl_csv_to_json_flex.DEF_SUB_RESOURCES: {
            'names': [
                [
                    {'resource_field': 'name', 'column': 'option_code_description'},
                    {'resource_field': 'locale', 'value': 'en'},
                    {'resource_field': 'locale_preferred', 'value': 'True'},
                    {'resource_field': 'name_type', 'value': 'Fully Specified'},
                ],
            ],
        },
        ocl_csv_to_json_flex.DEF_KEY_VALUE_PAIRS: {
            'extras': [
                {'key': 'option_code', 'value_column': 'option_code'},
                {'key': 'option_set', 'value_column': 'option_set'},
            ]
        }
    },
    {
        'definition_name': 'DATIM-TieredSupportOptions-Mapping',
        'is_active': True,
        'resource_type': 'Mapping',
        'skip_if_empty_column': 'id',
        ocl_csv_to_json_flex.DEF_CORE_FIELDS: [
            {'resource_field': 'owner', 'value': 'PEPFAR'},
            {'resource_field': 'owner_type', 'value': 'Organization'},
            {'resource_field': 'source', 'value': 'Tiered-Site-Support'},
            {'resource_field': 'from_concept_url', 'value': '/orgs/PEPFAR/sources/Tiered-Site-Support/concepts/SITE-TIERED-SUPPORT/'},
            {'resource_field': 'map_type', 'value': 'Has Option'},
            {'resource_field': 'to_concept_url', 'csv_to_json_processor': 'build_to_concept_url'},
        ]
    }
]

class csv2json_TieredSupport(ocl_csv_to_json_flex):
    def build_to_concept_url(self, csv_row, field_def):
        return '/orgs/PEPFAR/sources/Tiered-Site-Support/concepts/%s/' % csv_row['id']

csv_converter_data_elements = csv2json_TieredSupport(
    output_filename, csv_filename_dataelements, csv_resource_defs_dataelements, verbose=0)
csv_converter_data_elements.process_by_definition()
csv_converter_options = csv2json_TieredSupport(
    output_filename, csv_filename_options, csv_resource_defs_options, verbose=0)
csv_converter_options.process_by_definition()
