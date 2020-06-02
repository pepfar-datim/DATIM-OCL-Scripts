## Installation

Please run `pip install -r requirements.txt` to install the required dependencies

### Environment setup
The settings for these scripts can be hard-coded or set as environment variables:
 ```
 dhis2env = os.environ['DHIS2_ENV'] # DHIS2 Environment URL
 dhis2uid = os.environ['DHIS2_USER'] # DHIS2 Authentication USER
 dhis2pwd = os.environ['DHIS2_PASS'] # DHIS2 Authentication PASSWORD
 oclapitoken = os.environ['OCL_API_TOKEN'] # OCL Authentication API
 oclenv = os.environ['OCL_ENV'] # DHIS2 Environment URL
 compare2previousexport = os.environ['COMPARE_PREVIOUS_EXPORT'] in ['true', 'True']  # Whether to compare to previous export
 ```

 You need to specify whether you want to use the environmental variables or not and pass that as a command line argument. Example -
 ```
 python sims-sync.py true
 ```

## Scripts
### Shared
* settings.py

### IMAP
* imapexport.py
* imapimport.py
* showmoh.py
* MOH Sync Scripts
    * syncmoh.py
    * syncmohfy18.py
    * syncmohfy19.py
* Test scripts:
    * imaptest.py
    * imaptestcompareocl2csv.py
    * imaptestmediator.py

### QMAP
* exportqmap.py
* importqmap.py

### Supporting scripts
* bulkImportStatus.py
* celeryconfig.py
* constants.py
* import_manager.py
* import_util.py
* status_util.py

### Older scripts
* showmechanisms.py
* showmer.py
* showsims.py
* showtieredsupport.py
* syncmechanisms.py
* syncmer.py
* syncmermsp.py
* syncsims.py
* synctest.py