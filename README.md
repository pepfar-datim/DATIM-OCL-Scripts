## Overview
Sripts to import/export a country Indicator Mapping (IMAP) into/from OCL.

## Installation

Run `pip install -r requirements.txt` to install the required dependencies. Even though installing dependencies in global environment works, it's always advisable to create a virtual environment and install the required dependencies there to avoid potential package conflicts. 
Note that if using a virtual environment, openhim imap mediator (https://github.com/pepfar-datim/openhim-mediator-imap-import/blob/master/src/index.js) needs to use the created virtual environment.

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
python imapdiff.py --help
python imapbackup.py --help
python imaprestore.py --help
```
### Examples
```
# Get list of available PEPFAR DAA codelists (e.g. one per reporting cycle)
python showmoh.py --env=qa

# Get details of a specific codelist, e.g. DAA-FY21
python showmoh.py --env=qa -p=DAA-FY21

# Get list of country IMAPs loaded in the target OCL environment
python getimaporgs.py --env=qa -t=[your-ocl-api-token-here]

# Import a country IMAP
python imapimport.py -c=TEST -p=DAA-FY21 --env=qa -t=[your-ocl-api-token-here] -v2 imap-samples/DEMO-DAA-FY21.json

# Export a country IMAP
python imapexport.py -c=TEST -p=DAA-FY21 --env=qa -t=[your-ocl-api-token-here] -v2

# Compare an imported country IMAP with the original file
python imapdiff.py --env=qa -c=TEST -p=DAA-FY21 -t=[your-ocl-api-token-here] imap-samples/DEMO-DAA-FY21.json

# Backup all IMAPs in a target OCL environment
python imapbackup.py --env=qa -t=[your-ocl-api-token-here] > my-imap-backup-file.json

# Restore IMAPs to a target OCL environment (note this will overwrite existing IMAPs)
python imaprestore.py --env=qa -t=[your-ocl-api-token-here] my-imap-backup-file.json
```

## Scripts
### Configuration
* `settings.py` - Configure environment variables here
* `settings.blank.py` - A blank settings file if you're starting from scratch
* `requirements.txt` - Required python packages

### New OCL environment or codelist setup
* `importinit.py` - Use to load content, e.g. a codelist for a new reporting cycle
* `init/dhis2_moh_csv_to_ocl_json.py` - Script to transform a DHIS2 CSV codelist to an
  OCL-formatted JSON. This needs to be used for each reporting cycle.
* `init/*` - Sample JSON to start a project (e.g. DAA-FY22 codelist: datim_moh_fy22_daa.json)

### Command-line scripts
* IMAP Import and Export
    * `imapimport.py` - Import an IMAP into a target OCL environment
    * `imapexport.py` - Export an IMAP from a target OCL environment
* Helper functions
    * `showmoh.py` - Get a PEPFAR MOH Alignment codelist (e.g. DAA-FY22)
    * `getimaporgs.py` - Get a list of countries and or a list of IMAPs
    * `imapdiff.py` - Generate a diff between 2 IMAPs
* Backup and restore (to work with multiple IMAPs)
    * `imapbackup.py` - Backup a set of IMAPs into a single file from a target OCL environment
    * `imaprestore.py` - Restore a saved IMAP backup file to a target OCL environment
    * `imapdiffbackup.py` - Generate a diff between 2 IMAP backup files

### Sample IMAPs
* `imap-samples/*` - Sample IMAP JSON and CSV files

### Test scripts:
* `imaptest.py` - A generic script to easily run a batch of tests on IMAP resources
* `imaptestcompareocl2csv.py` - Test script to compare IMAP from OCL to an IMAP stored in a file
* `imaptestmediator.py` - Test script to export an IMAP using a mediator

### Supporting code
* `datim/*` - Business logic for working with IMAPs
* `common.py` - A few shared functions used by all of the command-line scripts
* `utils/*` - A few utility scripts if you need to work directly with an environment
* A few other old scripts: `status_util.py`, `iol.py`, `oclPassThroughReqeusts.py`, `status_util.py`
