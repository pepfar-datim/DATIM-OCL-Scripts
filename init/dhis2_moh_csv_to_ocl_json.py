"""
Converts a DSHI2 formatted CSV codelist to an OCL-formatted JSON bulk import script.
This has been tested and used for DAA-FY21 and CS-FY21. This would likely work for other
periods (eg FY20) as well with minimal modification.

1. Save DHIS2 codelist to CSV file (eg dhis2_moh_FY21_DAA.csv)
    CS-FY21: https://test.geoalign.datim.org/api/sqlViews/ioG5uxOYnZe/data.html+css?var=dataSets:u069j8gVYTA
    DAA-FY21: https://test.geoalign.datim.org/api/sqlViews/ioG5uxOYnZe/data.html+css?var=dataSets:U7qYX49krHK
    DAA-FY22: https://test.geoalign.datim.org/api/sqlViews/ioG5uxOYnZe/data.html+css?var=dataSets:RGDmmG5taRt
2. Create/update indicator mapping file, e.g. cs_fy21_indicator_maps.csv -- indicator code is
   extracted from the first 2 segments of a data element code by default; use the mapping
   file if the default setting is incorrect for a data element
3. Update script settings for the codelist
4. Run the script and save the outputted OCL bulk import script to a json file:
    python dhis2_moh_csv_to_ocl_json.py > datim_moh_fy21_daa.json
5. To load the codelist into an OCL environment, update and run importinit.py:
    python importinit.py
"""
# import unicodecsv as csv
import csv
import json
import ocldev.oclresourcelist


# Script settings
modality = 'DAA'  # e.g. CS or DAA
fy_only = 'FY22Q1'  # e.g. FY21 or FY22Q1
period = "DAA-FY22Q1"  # e.g. DAA-FY22Q1, CS-FY21
csv_filename = 'dhis2_moh_FY22_DAA.csv'  # e.g. dhis2_moh_FY22_DAA.csv
FILENAME_MAP_DE_CODE_TO_INDICATOR_CATEGORY = ''  # e.g. cs_fy21_indicator_maps.csv
owner = 'PEPFAR'  # e.g. PEPFAR
owner_type = 'Organization'  # e.g. Organization or User
debug = False


# Source, null disag, and source version configuration
datim_moh_source = {
    "type": "Source",
    "id": "DATIM-MOH-%s" % period,
    "short_code": "DATIM-MOH-%s" % period,
    "name": "DATIM-MOH-%s" % period,
    "full_name": "DATIM MOH %s %s Country Alignment Code List" % (modality, fy_only),
    "owner_type": owner_type,
    "owner": owner,
    "description": "",
    "extras": {
        "Period": period,
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
    "id": "%s.v0" % period,
    "description": "Initial release",
    "released": True,
    "source": datim_moh_source['id'],
    "owner": datim_moh_source['owner'],
    "owner_type": datim_moh_source['owner_type']
}


def find_nth(haystack, needle, n):
    start = haystack.find(needle)
    while start >= 0 and n > 1:
        start = haystack.find(needle, start+len(needle))
        n -= 1
    return start


def row_to_coc_concept(row, datim_moh_source):
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
    if row['code'] in map_de_code_to_indicator_category:
        indicator_category_code = map_de_code_to_indicator_category[row['code']]
        if debug:
            print(row['code'], indicator_category_code)
    else:
        indicator_category_code = row['code'][:find_nth(row['code'], '_', 2)]
        if debug:
            print('***', row['code'], indicator_category_code)
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


# Load mapping between indicator categories and data element codes
de_concepts = {}
coc_concepts = {}
de_coc_mappings = []
map_de_code_to_indicator_category = {}
if FILENAME_MAP_DE_CODE_TO_INDICATOR_CATEGORY:
    with open(FILENAME_MAP_DE_CODE_TO_INDICATOR_CATEGORY) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            map_de_code_to_indicator_category[row['de_code']] = row['indicator_category_code']

# Build the individual concepts and mappings
de_codes = {}
missing = []
found = []
if debug:
    print('LIST OF UNIQUE DATA ELEMENT CODES MAPPED TO INDICATOR CODES:')
    print('de_name,indicator_code')
with open(csv_filename) as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        de_code = row['code']
        if de_code not in de_codes:
            de_codes[de_code] = True
            found.append(de_code)
        if row["code"] not in de_concepts:
            de_concepts[row["code"]] = row_to_de_concept(
                row, datim_moh_source, map_de_code_to_indicator_category, debug)
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

if debug:
    print('\n\nCODELIST AS OCL-FORMATTED JSON:')

# Output
for resource in moh_resources:
    print(json.dumps(resource))
