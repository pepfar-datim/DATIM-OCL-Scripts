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
			{'resource_field':'from_concept_url', 'csv_to_json_processor':'build_from_concept_url'},
			{'resource_field':'map_type', 'value':'Has Option'},
			{'resource_field':'to_concept_url', 'csv_to_json_processor':'build_to_concept_url'},
			{'resource_field':'owner', 'value':'PEPFAR'},
			{'resource_field':'owner_type', 'value':'Organization'},
			{'resource_field':'source', 'value':'MER-Disaggregation'},
		]
	},
]


class mer_disaggregation_csv_converter(ocl_csv_to_json_flex):
	def build_from_concept_url(self, csv_row, field_def):
		# from the indicator...
		url = '/orgs/PEPFAR/sources/MER-Indicator/concepts/' + csv_row['code'] + '/'
		return url

	def build_to_concept_url(self, csv_row, field_def):
		# ...to the disaggregation option
		url = '/orgs/PEPFAR/sources/MER-Disaggregation/concepts/' + csv_row['categoryoptioncombocode'] + '/'
		return url

    def process_reference(self, csv_row, field_def):
        result = None
        #print field_def['data_column']
        if ('data_column' in field_def and field_def['data_column'] and
                field_def['data_column'] in csv_row):
            result = {'expressions': [ csv_row[field_def['data_column']] ]}
        return result


csv_converter = mer_disaggregation_csv_converter(csv_filename, csv_resource_definitions, verbose=False)
#csv_converter.process_by_definition()
csv_converter.process_by_row(30)


