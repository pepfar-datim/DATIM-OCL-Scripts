'''
Script to convert DATIM Mechanisms

This script defines the CSV Resource Definitions that are
passed on to the ocl_csv_to_json_flex converter. The resulting
JSON is intended for the json_flex_import and is not suitable
for the low-level concept/mapping importer.
'''
from csv_to_json_flex import ocl_csv_to_json_flex


csv_filename = 'MER_Indicator.csv'
output_filename = 'MER_Indicator.json'

csv_resource_definitions = [
	{
		'definition_name':'MER-Indicator',
		'is_active': True,
		'resource_type':'Concept',
		'id_column':'code',
		'skip_if_empty_column':'code',
		ocl_csv_to_json_flex.DEF_CORE_FIELDS:[
			{'resource_field':'concept_class', 'value':'Indicator'},
			{'resource_field':'datatype', 'value':'None'},
			{'resource_field':'external_id', 'column':'dataelementuid'},
			{'resource_field':'owner', 'value':'PEPFAR'},
			{'resource_field':'owner_type', 'value':'Organization'},
			{'resource_field':'source', 'value':'MER-Indicator'},
		],
		ocl_csv_to_json_flex.DEF_SUB_RESOURCES:{
			'names':[
				[
					{'resource_field':'name', 'column':'dataelement'},
					{'resource_field':'locale', 'value':'en'},
					{'resource_field':'locale_preferred', 'value':'True'},
					{'resource_field':'name_type', 'value':'Fully Specified'},
				],
				[
					{'resource_field':'name', 'column':'shortname'},
					{'resource_field':'locale', 'value':'en'},
					{'resource_field':'locale_preferred', 'value':'False'},
					{'resource_field':'name_type', 'value':'Short'},
				],
			],
			'descriptions':[]
		},
	},
]

csv_converter = ocl_csv_to_json_flex(output_filename, csv_filename, csv_resource_definitions, verbose=False)
#csv_converter.process_by_definition()
csv_converter.process_by_row()
