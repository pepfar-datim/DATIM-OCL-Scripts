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