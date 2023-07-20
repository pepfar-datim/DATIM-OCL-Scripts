## Overview
Sripts to import/export a country Indicator Mapping (IMAP) into/from OCL.

## Installation

Please run `pip install -r requirements.txt` to install the required dependencies

### Environment setup
1. Create a `data/` folder (in the root of the project folder) that the python scripts have write access to
2. Set environment configuration in `settings.py`. Some settings may be hard-coded or set as environment variables:
```
# OCL Authentication API
oclapitoken = os.environ['OCL_API_TOKEN']

# OCL Environment URL
oclenv = os.environ['OCL_ENV']

# Whether to compare to previous export before import
compare2previousexport = os.environ['COMPARE_PREVIOUS_EXPORT'] in ['true', 'True'] 
```

## Usage
### Command-line help
Many of these scripts are setup to work on the command line and provide command-line help:
```
python imapimport.py --help
python imapexport.py --help
python showmoh.py --help
python getimaporgs.py --help
```
### Examples
```
# Get list of available codelists
python showmoh.py --env=qa

# Get details of a specific codelist: DAA-FY21
python showmoh.py --env=qa -p=DAA-FY21

# Get list of IMAPs loaded in the target OCL environment
python getimaporgs.py --env=qa -t=[your-ocl-api-token-here]

# Import a codelist
python imapimport.py -c=TEST -p=DAA-FY21 --env=qa -t=[your-ocl-api-token-here] -v2 imap-samples/DEMO-DAA-FY21.json

# Export a codelist
python imapexport.py -c=TEST -p=DAA-FY21 --env=qa -t=[your-ocl-api-token-here] -v2

# Compare the imported IMAP with the original file
python imapdiff.py --env=qa -c=TEST -p=DAA-FY21 -t=[your-ocl-api-token-here] imap-samples/DEMO-DAA-FY21.json
```

## Scripts
### Configuration
* `settings.py` - Configure environment variables here

### New OCL environment or codelist setup
* `importinit.py` - Use to load content, e.g. a codelist for a new reporting cycle
* `init/*` - Sample scripts

### Command-line scripts
* IMAP Import and Export
    * `imapimport.py` - Import an IMAP into a target OCL environment
    * `imapexport.py` - Export an IMAP from a target OCL environment
* Helper functions
    * `showmoh.py` - Get a PEPFAR MOH Alignment codelist (e.g. DAA-FY22)
    * `getimaporgs.py` - Get a list of countries and or a list of IMAPs
    * `imapbackup.py` - Backup a set of IMAPs into a single file from a target OCL environment
    * `imaprestore.py` - Restore a saved IMAP backup file to a target OCL environment
    * `imapdiff.py` - Generate a diff between 2 IMAPs
    * `imapdiffbackup.py` - Generate a diff between 2 IMAP backup files

### Test scripts:
* `imaptest.py` - 
* `imaptestcompareocl2csv.py` - 
* `imaptestmediator.py` - 

### Supporting code
* `common.py` - 
* `datim/*` - 
* `requirements.txt` - 
* `settings.py` and `settings.blank.py` - 

### Sample IMAPs
* `imap-samples/*`

### Not sure if these are still used
* status_util.py
* iol.py
* oclPassThroughReqeusts.py
* status_util.py
* utils/*
