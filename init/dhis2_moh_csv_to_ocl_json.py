"""
Converts a DHIS2-formatted CSV codelist to an OCL-formatted JSON bulk import script.
This script was used for DAA-FY21, CS-FY21, DAA-FY22, and DAA-FY23, and it would
likely work for earlier periods (eg FY20) or non-DAA codelists with minimal modification.

Columns expected in the DHIS2-formatted CSV codelist: dataset, dataelement, shortname,
code, dataelementuid, dataelementdesc, categoryoptioncombo, categoryoptioncombocode,
categoryoptioncombouid, classification, indicator_category_code (Optional). Any additional
columns are ignored.

DAA-FY22 introduced a new optional column for "indicator_category_code" in the DHIS2 codelist
CSV. If this column is defined, it is automatically used to define the indicator_category_code
value for each data element. If it is not available, then an optional mapping file may be
used (see step 2 below for more details). Otherwise, the indicator_category_code is
automatically extracted from the first 2 segments of a data element code. For example, for
the data element "HTS_TST_N_MOH_Age_Agg_Sex_Result", the indicator code is set to "HTS_TST".
In most cases, the default behavior of automatically extracting the indicator code is all that
is necessary. Use the mapping file only if the default behavior is incorrect for a data element.

1. Save DHIS2 codelist to CSV file (eg dhis2_moh_FY21_DAA.csv), eg:
    CS-FY21: https://test.geoalign.datim.org/api/sqlViews/ioG5uxOYnZe/data.html+css?var=dataSets:u069j8gVYTA
    DAA-FY21: https://test.geoalign.datim.org/api/sqlViews/ioG5uxOYnZe/data.html+css?var=dataSets:U7qYX49krHK
    DAA-FY22: https://test.geoalign.datim.org/api/sqlViews/ioG5uxOYnZe/data.html+css?var=dataSets:RGDmmG5taRt
    DAA-FY23: https://geoalign.datim.org/api/sqlViews/ioG5uxOYnZe/data.html+css?var=dataSets:nPEPnHrNsnP
2. (Optional) Create/update indicator mapping file to link data elements to standardized
   indicator codes. For an example, see `cs_fy21_indicator_maps.csv`. By default, the
   indicator code is extracted from the first 2 segments of a data element code.
   Therefore, for the data element "HTS_TST_N_MOH_Age_Agg_Sex_Result", the indicator
   code is set to "HTS_TST".
3. Update Script Settings for the codelist (MODALITY, FY_ONLY, PERIOD, CSV_FILENAME, etc.)
4. Run the script and save the outputted OCL bulk import script to a json file:
    python dhis2_moh_csv_to_ocl_json.py > datim_moh_fy21_daa.json
5. To load the codelist into an OCL environment, update and run importinit.py:
    python importinit.py
"""
import csv
import json
import ocldev.oclresourcelist


# Script settings - Update these settings every time
MODALITY = 'DAA'  # e.g. CS or DAA
FY_ONLY = 'FY23'  # e.g. FY21 or FY22Q1
PERIOD = "DAA-FY23"  # e.g. DAA-FY21, DAA-FY22Q1, CS-FY21
CSV_FILENAME = 'dhis2_moh_fy23_daa.csv'  # e.g. dhis2_moh_FY22_DAA.csv
FILENAME_MAP_DE_CODE_TO_INDICATOR_CATEGORY = ''  # e.g. cs_fy21_indicator_maps.csv
OWNER = 'PEPFAR'  # e.g. PEPFAR
OWNER_TYPE = 'Organization'  # e.g. Organization or User
DEBUG = False


# DO NOT EDIT BELOW THIS LINE

# Source, null disag, and source version configuration
datim_moh_source = {
    "type": "Source",
    "id": "DATIM-MOH-%s" % PERIOD,
    "short_code": "DATIM-MOH-%s" % PERIOD,
    "name": "DATIM-MOH-%s" % PERIOD,
    "full_name": "DATIM MOH %s %s Country Alignment Code List" % (MODALITY, FY_ONLY),
    "owner_type": OWNER_TYPE,
    "owner": OWNER,
    "description": "",
    "extras": {
        "Period": PERIOD,
        "datim_moh_codelist": True
    },
    "default_locale": "en",
    "source_type": "Code List",
    "public_access": "View",
    "supported_locales": "en",
    "custom_validation_schema": "None"
}
null_disag_concept = {
    "type": "Concept",
    "id": "null-disag",
    "datatype": "None",
    "concept_class": "Disaggregate",
    "names": [{
        "locale": "en",
        "locale_preferred": "True",
        "name": "Null Disaggregate Option",
        "name_type": "Fully Specified"}],
    "source": datim_moh_source['id'],
    "owner": datim_moh_source['owner'],
    "owner_type": datim_moh_source['owner_type']
}
source_version = {
    "type": "Source Version",
    "id": "%s.v0" % PERIOD,
    "description": "Initial release",
    "released": True,
    "source": datim_moh_source['id'],
    "owner": datim_moh_source['owner'],
    "owner_type": datim_moh_source['owner_type']
}


def find_nth(haystack, needle, n):
    ''' Return index of nth occurence of a search string '''
    start = haystack.find(needle)
    while start >= 0 and n > 1:
        start = haystack.find(needle, start+len(needle))
        n -= 1
    return start


def row_to_coc_concept(row, datim_moh_source):
    ''' Generate category option combo (COC) concept from a codelist row '''
    concept = {
        "type": "Concept",
        "id": row['categoryoptioncombouid'],
        "retired": False,
        "datatype": "None",
        "external_id": row['categoryoptioncombouid'],
        "concept_class": "Disaggregate",
        "source": datim_moh_source["id"],
        "extras": {"classification": row['classification']},
        "descriptions": None,
        "owner": datim_moh_source['owner'],
        "owner_type": datim_moh_source['owner_type'],
        "names": [
            {
                "name": row["categoryoptioncombo"],
                "locale": "en",
                "locale_preferred": True,
                "name_type": "Fully Specified"
            }
        ]
    }
    return concept


def row_to_de_concept(row, datim_moh_source, map_de_code_to_indicator_category, debug=False):
    ''' Generate data element (DE) concept from row in the codelist '''

    # Determine the indicator_category_code for the concept
    if 'indicator_category_code' in row and row['indicator_category_code']:
        indicator_category_code = row['indicator_category_code']
    elif row['code'] in map_de_code_to_indicator_category:
        indicator_category_code = map_de_code_to_indicator_category[row['code']]
        if debug:
            print(row['code'], indicator_category_code)
    else:
        indicator_category_code = row['code'][:find_nth(row['code'], '_', 2)]
        if debug:
            print('***', row['code'], indicator_category_code)

    # Build the OCL-formatted concept
    concept = {
        "type": "Concept",
        "id": row['code'],
        "retired": False,
        "datatype": "Numeric",
        "external_id": row['dataelementuid'],
        "concept_class": "Data Element",
        "source": datim_moh_source["id"],
        "extras": {
            "indicator_category_code": indicator_category_code
        },
        "descriptions": [
            {
                "description": row["dataelementdesc"],
                "locale": "en",
                "locale_preferred": True,
                "description_type": "Description"
            }
        ],
        "owner": datim_moh_source['owner'],
        "owner_type": datim_moh_source['owner_type'],
        "names": [
            {
                "name": row["dataelement"],
                "locale": "en",
                "locale_preferred": True,
                "name_type": "Fully Specified"
            },
            {
                "name": row["shortname"],
                "locale": "en",
                "locale_preferred": False,
                "name_type": "Short"
            }
        ]
    }
    return concept


def row_to_mapping(row, datim_moh_source):
    ''' Generate a COC to DE mapping from a codelist row '''
    from_concept_url = '/orgs/%s/sources/%s/concepts/%s/' % (
        datim_moh_source['owner'], datim_moh_source['id'], row['code'])
    to_concept_url = '/orgs/%s/sources/%s/concepts/%s/' % (
        datim_moh_source['owner'], datim_moh_source['id'], row['categoryoptioncombouid'])
    mapping = {
        "retired": False,
        "type": "Mapping",
        "source": datim_moh_source['id'],
        "owner": datim_moh_source['owner'],
        "owner_type": datim_moh_source['owner_type'],
        "map_type": "Has Option",
        "from_concept_url": from_concept_url,
        "to_concept_url": to_concept_url
    }
    return mapping


# Load mapping between indicator categories and data element codes, if present
# NOTE: This is not used if an "indicator_category_code" column is present in the codelist
de_concepts = {}
coc_concepts = {}
de_coc_mappings = []
map_de_code_to_indicator_category = {}
if FILENAME_MAP_DE_CODE_TO_INDICATOR_CATEGORY:
    if DEBUG:
        print('LOADING INDICATOR MAPPING FILE: %s' % FILENAME_MAP_DE_CODE_TO_INDICATOR_CATEGORY)
    with open(FILENAME_MAP_DE_CODE_TO_INDICATOR_CATEGORY) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            map_de_code_to_indicator_category[row['de_code']] = row['indicator_category_code']

# Build the individual concepts and mappings
de_codes = {}
missing = []
found = []
if DEBUG:
    print('LIST OF UNIQUE DATA ELEMENT CODES MAPPED TO INDICATOR CODES:')
    print('de_name,indicator_code')
with open(CSV_FILENAME) as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        de_code = row['code']
        if de_code not in de_codes:
            de_codes[de_code] = True
            found.append(de_code)
        if row["code"] not in de_concepts:
            de_concepts[row["code"]] = row_to_de_concept(
                row, datim_moh_source, map_de_code_to_indicator_category, DEBUG)
        if row["categoryoptioncombouid"] not in coc_concepts:
            coc_concepts[row["categoryoptioncombouid"]] = row_to_coc_concept(row, datim_moh_source)
        de_coc_mappings.append(row_to_mapping(row, datim_moh_source))

# Build the resource list
moh_resources = ocldev.oclresourcelist.OclJsonResourceList()
moh_resources += datim_moh_source
moh_resources += null_disag_concept
for de_concept_id in de_concepts:
    moh_resources += de_concepts[de_concept_id]
for coc_concept_id in coc_concepts:
    moh_resources += coc_concepts[coc_concept_id]
moh_resources += de_coc_mappings
moh_resources += source_version

if DEBUG:
    print('\n\nCODELIST AS OCL-FORMATTED JSON:')

# Output
for resource in moh_resources:
    print(json.dumps(resource))
