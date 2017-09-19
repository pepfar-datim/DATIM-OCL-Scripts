from __future__ import with_statement
import os
import itertools, functools, operator
import requests
import sys
import json
from pprint import pprint
import tarfile
from deepdiff import DeepDiff
from requests.auth import HTTPBasicAuth


__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))


def attachAbsolutePath(filename):
    absolutefilename=os.path.join(__location__, filename)
    return absolutefilename


# TODO: still needs OCL authentication
def getRepositories(url='', oclenv='', oclapitoken='', key_field='id', verbosity=0,
                    require_external_id=True, active_attr_name='__datim_sync'):
    r = requests.get(url)
    r.raise_for_status()
    repos = r.json()
    filtered_repos = {}
    for r in repos:
        if (not require_external_id or ('external_id' in r and r['external_id'])) and (not active_attr_name or (r['extras'] and active_attr_name in r['extras'] and r['extras'][active_attr_name])):
            filtered_repos[r[key_field]] = r
    return filtered_repos


# Perform quick comparison of two files to determine if they have exactly the same size and contents
def filecmp(filename1, filename2):
    ''' Do the two files have exactly the same size and contents? '''
    try:
        with open(filename1, "rb") as fp1, open(filename2, "rb") as fp2:
            if os.fstat(fp1.fileno()).st_size != os.fstat(fp2.fileno()).st_size:
                return False # different sizes therefore not equal
            fp1_reader = functools.partial(fp1.read, 4096)
            fp2_reader = functools.partial(fp2.read, 4096)
            cmp_pairs = itertools.izip(iter(fp1_reader, ''), iter(fp2_reader, ''))
            inequalities = itertools.starmap(operator.ne, cmp_pairs)
            return not any(inequalities)
    except:
        return False


def saveDhis2SimsAssessmentTypesToFile(str_active_dataset_ids='', filename='', verbosity=0,
                                       dhis2env='', dhis2uid='', dhis2pwd=''):
    url_dhis2_export = dhis2env + 'api/dataElements.json?fields=name,code,id,valueType,lastUpdated,dataElementGroups[id,name]&order=code:asc&paging=false&filter=dataElementGroups.id:in:[' + str_active_dataset_ids + ']'
    if verbosity:
        print 'DHIS2 SIMS Assessment Types Request URL:', url_dhis2_export
    r = requests.get(url_dhis2_export, auth=HTTPBasicAuth(dhis2uid, dhis2pwd))
    r.raise_for_status()
    with open(attachAbsolutePath(new_dhis2_export_filename), 'wb') as handle:
        for block in r.iter_content(1024):
            handle.write(block)
    return r.headers['Content-Length']


def dhis2oj_sims(inputfile='', outputfile='', sims_collections=None, verbosity=0):
    ''' Transform each DataElement to an OCL concept and each DataElementGroup to an OCL reference '''
    with open(attachAbsolutePath(inputfile), "rb") as ifile, open(attachAbsolutePath(outputfile), 'wb') as ofile:
        new_sims = json.load(ifile)
        num_concepts = 0
        num_references = 0
        output = []

        # Iterate through each DataElement and transform to an OCL-JSON concept
        for de in new_sims['dataElements']:
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
            #ofile.write(json.dumps(c))
            #ofile.write(',\n')3
            output.append(c)
            num_concepts += 1

            # Iterate through each DataElementGroup and transform to an OCL-JSON reference
            for deg in de['dataElementGroups']:
                collection_id = sims_collections[deg['id']]['id']
                r = {
                    'type':'Reference',
                    'owner':'PEPFAR',
                    'owner_type':'Organization',
                    'collection':collection_id,
                    'data':{"expressions": ['/orgs/PEPFAR/sources/SIMS/concepts/' + concept_id + '/']}
                }
                #ofile.write(json.dumps(r))
                #ofile.write(',\n')
                output.append(r)
                num_references += 1
        #ofile.write(']')
        ofile.write(json.dumps(output))

    if verbosity:
        print 'Export successfully transformed into %s concepts and %s references' % (num_concepts, num_references)
    return True


# endpoint must point to the repo endpoint only, e.g. '/orgs/myorg/sources/mysource/'
# Note that atleast version of the repo must be released and the export for that version already created
# Todo: Still needs to implement OCL authentication
def saveOclLatestExport(endpoint='', oclenv='', tarfilename='', jsonfilename='', verbosity=0):
    # Get the latest version of the repo
    url_latest_version = oclenv + endpoint + 'latest/'
    if verbosity:
        print '\tLatest version request URL:', url_latest_version
    r = requests.get(url_latest_version)
    r.raise_for_status()
    latest_version_attr = r.json()
    if verbosity:
        print '\tLatest version ID:', latest_version_attr['id']

    # Get the export
    url_export = oclenv + endpoint + latest_version_attr['id'] + '/export/'
    if verbosity:
        print '\tExport URL:', url_export
    r = requests.get(url_export)
    r.raise_for_status()
    with open(attachAbsolutePath(tarfilename), 'wb') as handle:
        for block in r.iter_content(1024):
            handle.write(block)
    if verbosity:
        print '\t%s bytes saved to "%s"' % (r.headers['Content-Length'], tarfilename)

    # Decompress the tar and rename
    tar = tarfile.open(attachAbsolutePath(tarfilename))
    tar.extractall(__location__)
    tar.close()
    os.rename(attachAbsolutePath('export.json'), attachAbsolutePath(jsonfilename))
    if verbosity:
        print '\tExport decompressed to "%s"' % (jsonfilename)

    return True


# Settings
verbosity = 1   # 0 = none, 1 = some, 2 = all
old_dhis2_export_filename = 'old_sims_export.json'
new_dhis2_export_filename = 'new_sims_export.json'
converted_filename = 'converted_sims_export.json'
if len(sys.argv) > 1 and sys.argv[1] in ['true', 'True']:
    dhis2env = os.environ['DHIS2_ENV']
    dhis2uid = os.environ['DHIS2_USER']
    dhis2pwd = os.environ['DHIS2_PASS']
    oclapitoken = os.environ['OCL_API_TOKEN']
    oclenv = os.environ['OCL_ENV']
    compare2previousexport = os.environ['COMPARE_PREVIOUS_EXPORT'] in ['true', 'True']
else:
    dhis2env = 'https://dev-de.datim.org/'
    dhis2uid = 'jpayne'
    dhis2pwd = 'Johnpayne1!'
    oclapitoken = '2da0f46b7d29aa57970c0b3a535121e8e479f881'
    oclenv = 'https://api.showcase.openconceptlab.org'
    compare2previousexport = True

url_sims_collections = oclenv + '/orgs/PEPFAR/collections/?q=SIMS&verbose=true'
ocl_export_defs = {
    'sims_source': {
        'endpoint':'/orgs/PEPFAR/sources/SIMS/',
        'tarfilename':'sims_source_ocl_export.tar',
        'jsonfilename':'sims_source_ocl_export.json',
        'jsoncleanfilename':'sims_source_ocl_export_clean.json',
    }
}


# Write settings to console
if verbosity:
    print '**** SIMS Sync Script Settings:'
    print '\tverbosity:', verbosity
    print '\tdhis2env:', dhis2env
    print '\toclenv:', oclenv
    print '\tcompare2previousexport:', compare2previousexport
    print '\told_dhis2_export_filename:', old_dhis2_export_filename
    print '\tnew_dhis2_export_filename:', new_dhis2_export_filename
    print '\tconverted_filename:', converted_filename


# STEP 1a: Fetch OCL Collections for SIMS Assessment Types
# Collections that have 'SIMS' in the name, __datim_sync==true, and external_id not empty
if verbosity:
    print '\n**** STEP 1a: Fetch OCL Collections for SIMS Assessment Types'
sims_collections = getRepositories(url=url_sims_collections, oclenv=oclenv,
                                   key_field='external_id', verbosity=verbosity)
if verbosity:
    print '\tCollections retreived:', len(sims_collections)
if verbosity >= 2:
    pprint(sims_collections)


'''
# STEP 1a-Alternate: Use local saved copy of OCL Collections for SIMS Assessment Types
# Save for offline use
offline_sims_collections_filename = 'sims_collections_empty.json'
#with open(offline_sims_collections_filename, 'wb') as handle:
#    handle.write(json.dumps(sims_collections, indent=4))

# OFFLINE: Load saved sims collections
with open(offline_sims_collections_filename, 'rb') as handle:
    sims_collections = json.load(handle)
'''


# STEP 1b: Compile list of DHIS2 dataset IDs from collection external_id
if verbosity:
    print '\n**** STEP 1b: Compile list of DHIS2 dataset IDs from collection external_id'
str_active_dataset_ids = ','.join(sims_collections.keys())
if verbosity:
    print '\tSIMS Assessment Type Dataset IDs:', str_active_dataset_ids


# Step 2: Fetch new export from DATIM DHIS2
if verbosity:
    print '\n**** STEP 2: Fetch new export from DATIM DHIS2'
content_length = saveDhis2SimsAssessmentTypesToFile(str_active_dataset_ids=str_active_dataset_ids,
                                                    filename=new_dhis2_export_filename,
                                                    dhis2env=dhis2env, dhis2uid=dhis2uid, dhis2pwd=dhis2pwd,
                                                    verbosity=verbosity)
if verbosity:
    print '%s bytes retrieved and written to file "%s"' % (content_length, new_dhis2_export_filename)


# STEP 3: Quick comparison of current and previous DHIS2 exports
# Compares new DHIS2 export to most recent previous export from a successful sync that is available
# Copmarison first checks file size then file contents
if verbosity:
    print '\n**** STEP 3: Quick comparison of current and previous DHIS2 exports'
if not compare2previousexport:
    if verbosity:
        print "Skipping (due to settings)..."
elif not old_dhis2_export_filename:
    if verbosity:
        print "Skipping (no previous export filename provided)..."
else:
    if filecmp(old_dhis2_export_filename, new_dhis2_export_filename):
        print "Current and previous exports are identical, so exit without doing anything..."
        sys.exit()
    else:
        print "Current and previous exports are different, so continue with synchronization..."


# STEP 4: Transform new DHIS2 export to OCL-formatted JSON (OJ)
# python dhis2oj-sims.py -i inputfile.xml -o outputfile.json -v1
if verbosity:
    print '\n**** STEP 4: Transform DHIS2 export to OCL formatted JSON'
dhis2oj_sims(inputfile=new_dhis2_export_filename, outputfile=converted_filename,
             sims_collections=sims_collections, verbosity=verbosity)


# STEP 5a: Fetch latest versions of relevant OCL exports
if verbosity:
    print '\n**** STEP 5a: Fetch latest versions of relevant OCL exports'
for k in ocl_export_defs:
    if verbosity:
        print '%s:' % (k)
    export_def = ocl_export_defs[k]
    saveOclLatestExport(endpoint=export_def['endpoint'], tarfilename=export_def['tarfilename'],
                        jsonfilename=export_def['jsonfilename'], oclenv=oclenv, verbosity=verbosity)


# STEP 5b: Prepare OCL exports for diff
# Concepts/mappings in OCL exports have extra attributes that should be removed before the diff
if verbosity:
    print '\n**** STEP 5b: Prepare OCL exports for diff'
with open(attachAbsolutePath(ocl_export_defs['sims_source']['jsonfilename']), 'rb') as ifile, open(attachAbsolutePath(ocl_export_defs['sims_source']['jsoncleanfilename']), 'wb') as ofile:
    ocl_sims_export = json.load(ifile)
    #if verbosity >= 2:
    #    pprint(ocl_sims_export)
    ocl_sims_export_clean = []
    for c in ocl_sims_export['concepts']:
        # clean the concept and write it
        ocl_sims_export_clean.append[c]
        #ofile.write(json.dumps(c))
    for m in ocl_sims_export['mappings']:
        # clean the mapping and write it
        ocl_sims_export_clean.append[m]
        #ofile.write(json.dumps(m))
    ofile.write(json.dumps(ocl_sims_export_clean))
if verbosity:
    print 'Success...'


# STEP 6a: Perform deep diff
# Note that multiple deep diffs may be performed, each with their own input and output files
if verbosity:
    print '\n**** STEP 6: Perform deep diff'
diff = {}
with open(ocl_export_defs['sims_source']['jsoncleanfilename'], 'rb') as ocl_handle, open(converted_filename, 'rb') as dhis2_handle:
    a_ocl = json.load(ocl_handle)
    b_dhis2 = json.load(dhis2_handle)
    diff = DeepDiff(a_ocl, b_dhis2, ignore_order=True, verbose_level=1)
    if verbosity:
        print 'Diff results:'
        for k in diff:
            print '\t%s:' % (k), len(diff[k])
    if verbosity >= 2:
        print json.dumps(diff, indent=4)


## IF DATA CHECK ONLY, THEN OUTPUT RESULT OF DIFF AND END HERE
# TODO: Need to handle diff result and send the right exit code
if diff:
    pass
else:
    print 'No diff, exiting...'
    exit()


# STEP 7: Generate import script
# Generate import script by processing the diff results
if verbosity:
    print '\n**** STEP 7: Generate import script'
if 'iterable_item_added' in diff:
    pass
if 'value_changed' in diff:
    pass


# STEP 8: Import the update script into ocl
# Parameters: testmode
#python oclimport.py -i importfile.json -v1 [--testmode=true] --ocltoken=... 1>oclimport-sims-stdout.log 2>oclimport-sims-stderr.log
if verbosity:
    print '\n**** STEP 8: Perform the import in OCL'


# STEP 9: Save new DHIS2 export for the next sync attempt
if verbosity:
    print '\n**** STEP 9: Save the DHIS2 export'


# STEP 10: Manage OCL repository versions
# create new version (maybe delete old version)
if verbosity:
    print '\n**** STEP 10: Manage OCL repository versions'

