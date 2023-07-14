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

## Scripts
### Configuration
* settings.py

### New OCL environment or codelist setup
* importinit.py - 
* init/*

### IMAP Import and Export
* imapimport.py
* imapexport.py

### Helper functions
* showmoh.py - Get a PEPFAR codelist (e.g. DAA-FY22)
* getimaporgs.py - Get a list of countries and or a list of IMAPs
* imapbackup.py - Backup a set of IMAPs into a single file from a target OCL environment
* imaprestore.py - Restore a saved IMAP backup file to a target OCL environment
* imapdiff.py - Generate a diff between 2 IMAPs
* imapdiffbackup.py - Generate a diff between 2 IMAP backup files

### Test scripts:
* imaptest.py
* imaptestcompareocl2csv.py
* imaptestmediator.py

### Supporting code
* common.py - 
* datim/* - 
* requirements.txt - 
* settings.py and settings.blank.py - 

### Sample IMAPs
* imap-samples/*

### Not sure if these are still used
* status_util.py
* iol.py
* oclPassThroughReqeusts.py
* status_util.py
* utils/*