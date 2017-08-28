'''
Script to convert Collections CSV file to OCL-formatted JSON

This script defines the CSV Resource Definitions that are
passed on to the ocl_csv_to_json_flex converter. The resulting
JSON is intended for the json_flex_import and is not suitable
for the low-level concept/mapping importer.
'''
from csv_to_json_flex import ocl_csv_to_json_flex


csv_filename = 'Datasets.csv'
output_filename = 'Datasets.json'


csv_resource_definitions = [
	{
		'definition_name':'Collections',
		'is_active': True,
		'resource_type':'Collection',
		'id_column':'code',
		'skip_if_empty_column':'code',
		ocl_csv_to_json_flex.DEF_CORE_FIELDS:[
			{'resource_field':'owner', 'value':'PEPFAR'},
			{'resource_field':'owner_type', 'value':'Organization'},
			{'resource_field':'name', 'column':'shortname'},
			{'resource_field':'full_name', 'column':'name'},
			{'resource_field':'description', 'column':'description'},
			{'resource_field':'default_locale', 'value':'en'},
			{'resource_field':'supported_locales', 'value':'en'},
			{'resource_field':'short_code', 'column':'code'},
			{'resource_field':'collection_type', 'value':'Subset'},
			{'resource_field':'public_access', 'value':'View'},
			{'resource_field':'external_id', 'column':'uid'},
		],
	},
]

csv_converter = ocl_csv_to_json_flex(output_filename, csv_filename, csv_resource_definitions, verbose=False)
csv_converter.process_by_definition()
