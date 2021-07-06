import unicodecsv as csv
import json
import ocldev.oclresourcelist


fy_only = 'FY21'
period = "CS-FY21"
csv_filename = 'dhis2_moh_fy21_cs.csv'
owner = 'PEPFAR'
owner_type = 'Organization'

de_concepts = {}
coc_concepts = {}
de_coc_mappings = []

datim_moh_source = {
    "type": "Source",
    "id": "DATIM-MOH-%s" % period,
    "short_code": "DATIM-MOH-%s" % period,
    "name": "DATIM-MOH-%s" % period,
    "full_name": "DATIM MOH Central Support %s Country Alignment Code List" % fy_only,
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
    "names": [{"locale": "en", "locale_preferred": "True", "name": "Null Disaggregate Option", "name_type": "Fully Specified"}],
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


def row_to_de_concept(row, datim_moh_source):
    concept = {
        "type": "Concept",
        "id": row['code'],
        "retired": False,
        "datatype": "Numeric",
        "external_id": row['dataelementuid'],
        "concept_class": "Data Element",
        "source": datim_moh_source["id"],
        "extras": {
            "indicator_category_code": row['code'][:find_nth(row['code'], '_', 2)]
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


# Build the individual concepts and mappings
with open(csv_filename) as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        # print json.dumps(row, indent=4)
        if row["code"] not in de_concepts:
            de_concepts[row["code"]] = row_to_de_concept(row, datim_moh_source)
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

# Output
for resource in moh_resources:
    print json.dumps(resource)
