from __future__ import with_statement
import os
import itertools, functools, operator
import requests
import sys
import json
import pprint
import tarfile
from requests.auth import HTTPBasicAuth


# TODO: still need authentication, oclenv parameter
def getRepositories(url='', oclenv='', oclapitoken='', key_field='id',
                    require_external_id=True, active_attr_name='__datim_sync'):
    r = requests.get(url)
    r.raise_for_status()
    collections = r.json()
    active_dataset_ids = []
    filtered_collections = {}
    for c in collections:
        if (not require_external_id or ('external_id' in c and c['external_id'])) and (not active_attr_name or (c['extras'] and active_attr_name in c['extras'] and c['extras'][active_attr_name])):
            filtered_collections[c[key_field]] = c
    return filtered_collections


# Perform quick comparison of two files by size then contents
def filecmp(filename1, filename2):
    "Do the two files have exactly the same contents?"
    try:
        with open(filename1, "rb") as fp1, open(filename2, "rb") as fp2:
            if os.fstat(fp1.fileno()).st_size != os.fstat(fp2.fileno()).st_size:
                return False # different sizes therefore not equal
            fp1_reader= functools.partial(fp1.read, 4096)
            fp2_reader= functools.partial(fp2.read, 4096)
            cmp_pairs= itertools.izip(iter(fp1_reader, ''), iter(fp2_reader, ''))
            inequalities= itertools.starmap(operator.ne, cmp_pairs)
            return not any(inequalities)
    except:
        return False


def saveDhis2SimsAssessmentTypesToFile(str_active_dataset_ids, filename, dhis2env, dhis2uid, dhis2pwd):
    url_dhis2_export = dhis2env + 'api/dataElements.json?fields=name,code,id,valueType,lastUpdated,dataElementGroups[id,name]&order=code:asc&paging=false&filter=dataElementGroups.id:in:[' + str_active_dataset_ids + ']'
    print url_dhis2_export
    r = requests.get(url_dhis2_export, auth=HTTPBasicAuth(dhis2uid, dhis2pwd))
    r.raise_for_status()
    with open(new_dhis2_export_filename, 'wb') as handle:
        for block in r.iter_content(1024):
            handle.write(block)


def dhis2oj_sims(inputfile, outputfile, sims_collections):
    with open(inputfile, "rb") as ifile, open(outputfile, 'wb') as ofile:
        new_sims = json.load(ifile)
        for de in new_sims['dataElements']:
            #print '\n\n', de
            concept_id = de['code']
            c = {
                'type':'Concept',
                'concept_id':concept_id,
                'concept_class':'Assessment Type',
                'datatype':'None',
                'owner':'PEPFAR',
                'owner_type':'Organization',
                'source':'SIMS',
                'external_id':de['id'],
                'names':[
                    {'name':de['name'], 'name_type':'Fully Specified', 'locale':'en'}
                ],
                'extras':{'Value Type':de['valueType']}
            }
            ofile.write(json.dumps(c))
            ofile.write('\n')
            for deg in de['dataElementGroups']:
                #print '\t', deg
                #if deg['id'] not in sims_collections:
                #    break
                collection_id = sims_collections[deg['id']]['id']
                r = {
                    'type':'Reference',
                    'owner':'PEPFAR',
                    'owner_type':'Organization',
                    'collection':collection_id,
                    'data':{"expressions": ['/orgs/PEPFAR/sources/SIMS/concepts/' + concept_id + '/']}
                }
                ofile.write(json.dumps(r))
                ofile.write('\n')


# endpoint must point to the repo endpoint only, e.g. '/orgs/myorg/sources/mysource/'
# Note that a version of the repo must be released and the export for that version already created
def saveOclLatestExport(endpoint='', oclenv='', tarfilename='', jsonfilename=''):
    # Get the latest version of the repo
    url_version = oclenv + endpoint + 'latest/'
    r = requests.get(url_version)
    r.raise_for_status()
    sims_version = r.json()

    # Get the export
    url_export = oclenv + endpoint + sims_version['id'] + '/export/'
    print 'Export:', url_export
    r = requests.get(url_export)
    r.raise_for_status()
    with open(tarfilename, 'wb') as handle:
        for block in r.iter_content(1024):
            handle.write(block)

    # Decompress the tar and rename
    tar = tarfile.open(tarfilename)
    tar.extractall()
    tar.close()
    os.rename('export.json', jsonfilename)


# Settings
old_dhis2_export_filename = 'old_sims_export.json'
new_dhis2_export_filename = 'new_sims_export.json'
converted_filename = 'converted_sims_export.json'
dhis2env = 'https://dev-de.datim.org/'
dhis2uid = 'jonpayne'
dhis2pwd = 'Jonpayne1'
oclapitoken = '2da0f46b7d29aa57970c0b3a535121e8e479f881' # 
oclenv = 'https://api.showcase.openconceptlab.org'
url_sims_collections = oclenv + '/orgs/PEPFAR/collections/?q=SIMS&verbose=true'
compare2previousexport = True
ocl_export_defs = {
    'sims_source': {
        'endpoint':'/orgs/PEPFAR/sources/SIMS/',
        'tarfilename':'sims_source_ocl_export.tar',
        'jsonfilename':'sims_source_ocl_export.json',
        'jsoncleanfilename':'sims_source_ocl_export_clean.json',
    }
}



# Step 1: Fetch list of active datasets from OCL collections with __datim_sync==true and external_id
sims_collections = getRepositories(url_sims_collections, oclenv=oclenv, key_field='external_id')
str_active_dataset_ids = ','.join(sims_collections.keys())


'''
# Save for offline use
offline_sims_collections_filename = 'sims_collections_empty.json'
#with open(offline_sims_collections_filename, 'wb') as handle:
#    handle.write(json.dumps(sims_collections, indent=4))

# OFFLINE: Load saved sims collections
with open(offline_sims_collections_filename, 'rb') as handle:
    sims_collections = json.load(handle)
'''

#print str_active_dataset_ids
#print sims_collections


# Step 2: Fetch new export from DHIS2
saveDhis2SimsAssessmentTypesToFile(str_active_dataset_ids, new_dhis2_export_filename,
                                   dhis2env, dhis2uid, dhis2pwd)


# Step 3: Compare new DHIS2 export to previous export that is available from a successful sync
if compare2previousexport and old_dhis2_export_filename:
    if filecmp(old_dhis2_export_filename, new_dhis2_export_filename):
        print "Files are the same, so exit without doing anything..."
        sys.exit()
    else:
        print "Files are different, so continue..."


# Step 4: Transform new DHIS2 export to OCL-formatted JSON (OJ)
# python dhis2oj-sims.py -i inputfile.xml -o outputfile.json -v1
dhis2oj_sims(new_dhis2_export_filename, converted_filename, sims_collections)


# Step 5a: Fetch latest versions of relevant OCL exports
for k in ocl_export_defs:
    export_def = ocl_export_defs[k]
    saveOclLatestExport(endpoint=export_def['endpoint'], tarfilename=export_def['tarfilename'],
                        jsonfilename=export_def['jsonfilename'], oclenv=oclenv)


# Step 5b: Prepare OCL exports for the diff
with open(ocl_export_defs['sims_source']['jsonfilename'], 'rb') as ifile, open(ocl_export_defs['sims_source']['jsoncleanfilename'], 'wb') as ofile:
    ocl_sims_export = json.load(ifile)
    for c in ocl_sims_export['concepts']:
        # clean the concept and write it
        ofile.write(json.dumps(c))
    for m in ocl_sims_export['mappings']:
        # clean the mapping and write it
        ofile.write(json.dumps(m))


'''
# Step 6: Generate import script by evaluating diff between new DHIS2 and OCL exports
# NOTE: Many input files may be necessary, so moving configuration into a json file makes more sense
python ocldiff-sims.py --i1=dhis2file.json --i2=oclfile.json -o importfile.json -v1 1>ocldiff-sims-stdout.log 2>ocldiff-sims-stderr.log

## IF DATA CHECK ONLY, THEN OUTPUT RESULT OF DIFF AND END HERE

if at least one diff:

    # Step 7: Import the update script into ocl
    # Parameters: testmode
    python oclimport.py -i importfile.json -v1 [--testmode=true] --ocltoken=... 1>oclimport-sims-stdout.log 2>oclimport-sims-stderr.log

    # Step 8: Save new DHIS2 export for the next sync attempt

    # Step 8: Manage OCL repository versions
    # create new version (maybe delete old version)

'''
