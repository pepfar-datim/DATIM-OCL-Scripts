
**Process**
1) Run MER_Indicators.py - expects input file named 'MER_Indicator.csv' containing list of unique MER Indicators by uid. Results in MER_Indicator.json file


**Changes Made**
File: csv_to_json_flex

1) Changed From print ('SKIPPING: '), csv_resource_def['definition_name']
TO print 'SKIPPING: ', csv_resource_def['definition_name']
2) Added argument and line for output JSON file

FILE: MER_Indicator etc...
1) process_by_row() function - remove 30 argument
2) _init_ - added output_filename argument