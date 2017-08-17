'''
Script to convert DATIM Mechanisms

This script defines the CSV Resource Definitions that are
passed on to the ocl_csv_to_json_flex converter. The resulting
JSON is intended for the json_flex_import and is not suitable
for the low-level concept/mapping importer.
'''
from csv_to_json_flex import ocl_csv_to_json_flex


csv_filename = 'Mechanisms partners agencies OUS Start End.csv'
output_filename = 'Mechanisms.json'

csv_resource_definitions = [
	{
		'definition_name':'Mechanism-Concept',
		'is_active': True,
		'resource_type':'Concept',
		'id_column':'code',
		'skip_if_empty_column':'code',
		ocl_csv_to_json_flex.DEF_CORE_FIELDS:[
			{'resource_field':'concept_class', 'value':'Funding Mechanism'},
			{'resource_field':'datatype', 'value':'Text'},
			{'resource_field':'external_id', 'column':'uid'},
			{'resource_field':'owner', 'value':'PEPFAR'},
			{'resource_field':'owner_type', 'value':'Organization'},
			{'resource_field':'source', 'value':'Mechanism'},
		],
		ocl_csv_to_json_flex.DEF_SUB_RESOURCES:{
			'names':[
				[
					{'resource_field':'name', 'column':'mechanism'},
					{'resource_field':'locale', 'value':'en'},
					{'resource_field':'locale_preferred', 'value':'True'},
					{'resource_field':'name_type', 'value':'Fully Specified'},
				],
			],
			'descriptions':[]
		},
		ocl_csv_to_json_flex.DEF_KEY_VALUE_PAIRS:{
			'extras': [
				{'key':'Partner', 'value_column':'partner'},
				{'key':'Prime Id', 'value_column':'primeid'},
				{'key':'Agency', 'value_column':'agency'},
				{'key':'Operational Unit', 'value_column':'ou'},
				{'key':'Start Date', 'value_column':'startdate'},
				{'key':'End Date', 'value_column':'enddate'},
			]
		}
	},
]

csv_converter = ocl_csv_to_json_flex(output_filename, csv_filename, csv_resource_definitions, verbose=False)
csv_converter.process_by_row()

