'''
Script to convert DATIM Dissagragte Mapping

This script defines the CSV Resource Definitions that are
passed on to the ocl_csv_to_json_flex converter. The resulting
JSON is intended for the json_flex_import and is not suitable
for the low-level concept/mapping importer.
'''
from csv_to_json_flex import ocl_csv_to_json_flex


csv_filename = 'MER_Indicator_Dissagregation_Mapping.csv'

csv_resource_definitions = [
	{
		'definition_name':'MER-Disaggregate-Mapping',
		'is_active': True,
		'resource_type':'Mapping',
		'id_column':None,
		'internal_external': {'value':ocl_csv_to_json_flex.INTERNAL_MAPPING_ID},
		ocl_csv_to_json_flex.DEF_CORE_FIELDS:[
			{'resource_field':'from_concept_url', 'column':'target_url'},
			{'resource_field':'map_type', 'value':'Has Option'},
			{'resource_field':'to_concept_url', 'column':'option_url'},
			{'resource_field':'owner', 'value':'PEPFAR'},
			{'resource_field':'owner_type', 'value':'Organization'},
			{'resource_field':'source', 'value':'MER Disaggregate'},
		]
	},
	{
		'definition_name':'MER-Indicator-Reference',
		'is_active': True,
		'resource_type':'Reference',
		'skip_if_empty_column':'code',
		ocl_csv_to_json_flex.DEF_CORE_FIELDS:[
			{'resource_field':'data', 'data_column':'target_url', 'csv_to_json_processor':'process_reference'},
			{'resource_field':'owner', 'value':'PEPFAR'},
			{'resource_field':'owner_type', 'value':'Organization'},
			{'resource_field':'collection', 'value':'MER-Indicator-COP17FY18-Community-Based'},
		]
	},
]

csv_converter = ocl_csv_to_json_flex(csv_filename, csv_resource_definitions, verbose=False)
#csv_converter.process_by_definition()
csv_converter.process_by_row(30)