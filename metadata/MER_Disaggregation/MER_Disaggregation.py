'''
Script to convert DATIM Mechanisms

This script defines the CSV Resource Definitions that are
passed on to the ocl_csv_to_json_flex converter. The resulting
JSON is intended for the json_flex_import and is not suitable
for the low-level concept/mapping importer.
'''
from csv_to_json_flex import ocl_csv_to_json_flex


csv_filename = 'MER_Disaggregation.csv'
output_filename = 'MER_Disaggregation.json'

csv_resource_definitions = [
{
		'definition_name':'MER-Disaggregate',
		'is_active': True,
		'resource_type':'Concept',
		'id_column':'categoryoptioncombocode',
		'skip_if_empty_column':'categoryoptioncombocode',
		ocl_csv_to_json_flex.DEF_CORE_FIELDS:[
			{'resource_field':'concept_class', 'value':'Varies'},
			{'resource_field':'datatype', 'value':'None'},
			{'resource_field':'external_id', 'column':'categoryoptioncombouid'},
			{'resource_field':'owner', 'value':'PEPFAR'},
			{'resource_field':'owner_type', 'value':'Organization'},
			{'resource_field':'source', 'value':'MER Disaggregations'},
		],
		ocl_csv_to_json_flex.DEF_SUB_RESOURCES:{
			'names':[
				[
					{'resource_field':'name', 'column':'categoryoptioncombo'},
					{'resource_field':'locale', 'value':'en'},
					{'resource_field':'locale_preferred', 'value':'True'},
					{'resource_field':'name_type', 'value':'Fully Specified'},
				],
			],
			'descriptions':[]
		},
	},
]
csv_converter = ocl_csv_to_json_flex(output_filename, csv_filename, csv_resource_definitions, verbose=False)
#csv_converter.process_by_definition()
csv_converter.process_by_row()
