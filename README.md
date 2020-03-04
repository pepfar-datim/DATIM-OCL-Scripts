**Installation**

Please run `pip install -r requirements.txt` to install the required imports


**Changes Made**
File: csv_to_json_flex

1) Changed From print ('SKIPPING: '), csv_resource_def['definition_name']
TO print 'SKIPPING: ', csv_resource_def['definition_name']
2) Added argument and line for output JSON file

FILE: MER_Indicator etc...
1) process_by_row() function - remove 30 argument
2) _init_ - added output_filename argument

### Running the script:
These scripts need a few variables set before it can run successfully, they can either be set as environmental variables or hard coded in the script. The variables needed are -
 ```
 dhis2env = os.environ['DHIS2_ENV'] # DHIS2 Environment URL
 dhis2uid = os.environ['DHIS2_USER'] # DHIS2 Authentication USER
 dhis2pwd = os.environ['DHIS2_PASS'] # DHIS2 Authentication PASSWORD
 oclapitoken = os.environ['OCL_API_TOKEN'] # OCL Authentication API
 oclenv = os.environ['OCL_ENV'] # DHIS2 Environment URL
 compare2previousexport = os.environ['COMPARE_PREVIOUS_EXPORT'] in ['true', 'True']  # Whether to compare to previous export
 ```

 You need to specify whether you want to use the environmental varialbes or not and pass that as a command line argument. Example -
 ```
 python sims-sync.py true
 ```
