"""
Script to convert Collections CSV file to OCL-formatted JSON
"""
from csv_to_json_flex import ocl_csv_to_json_flex

csv_filename = 'dhis2datasets.csv'
output_filename = 'dhis2datasets.jsonl'

csv_resource_definitions = [
    {
        'definition_name': 'DATIM-Collections',
        'is_active': True,
        'resource_type': 'Collection',
        'id_column': 'OCL: Collection',
        'skip_if_empty_column': 'OCL: Collection',
        ocl_csv_to_json_flex.DEF_CORE_FIELDS: [
            {'resource_field': 'owner', 'value': 'PEPFAR'},
            {'resource_field': 'owner_type', 'value': 'Organization'},
            {'resource_field': 'name', 'column': 'Dataset: shortname'},
            {'resource_field': 'full_name', 'column': 'Dataset: fullname'},
            {'resource_field': 'default_locale', 'value': 'en'},
            {'resource_field': 'supported_locales', 'value': 'en'},
            {'resource_field': 'short_code', 'column': 'OCL: Collection'},
            {'resource_field': 'collection_type', 'value': 'Subset'},
            {'resource_field': 'public_access', 'value': 'View'},
            {'resource_field': 'external_id', 'column': 'ZenDesk: Dataset'},
        ],
        ocl_csv_to_json_flex.DEF_KEY_VALUE_PAIRS: {
            'extras': [
                {'key': 'Period', 'value_column': 'OCL: Period', 'omit_if_empty_value': True},
                {'key': 'DHIS2-Dataset-Code', 'value_column': 'Dataset: code', 'omit_if_empty_value': True},
                {'key_column': 'OCL: Active Sync Attribute', 'value':True}
            ]
        }
    },
]

csv_converter = ocl_csv_to_json_flex(output_filename, csv_filename, csv_resource_definitions, verbose=0)
csv_converter.process_by_definition()
