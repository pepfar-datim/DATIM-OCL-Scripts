from __future__ import with_statement
import os
import itertools, functools, operator
import requests
import sys
import json
import tarfile
from pprint import pprint
from deepdiff import DeepDiff
from requests.auth import HTTPBasicAuth


__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))


def attachAbsolutePath(filename):
    ''' Adds full absolute path to the filename '''
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
        output = {}

        # Iterate through each DataElement and transform to an OCL-JSON concept
        for de in new_sims['dataElements']:
            concept_id = de['code']
            url = '/orgs/PEPFAR/sources/SIMS/concepts/' + concept_id + '/'
            c = {
                'type':'Concept',
                'id':concept_id,
                'concept_class':'Assessment Type',
                'datatype':'None',
                'owner':'PEPFAR',
                'owner_type':'Organization',
                'source':'SIMS',
                'retired':False,
                'descriptions':None,
                'external_id':de['id'],
                'names':[
                    {
                        'name':de['name'],
                        'name_type':'Fully Specified',
                        'locale':'en',
                        'locale_preferred':False,
                        'external_id':None,
                    }
                ],
                'extras':{'Value Type':de['valueType']}
            }
            output[url] = c
            num_concepts += 1

            # Iterate through each DataElementGroup and transform to an OCL-JSON reference
            for deg in de['dataElementGroups']:
                collection_id = sims_collections[deg['id']]['id']
                target_url = '/orgs/PEPFAR/sources/SIMS/concepts/' + concept_id + '/'
                url = '/orgs/PEPFAR/collections/' + collection_id + '/references/?target=' + target_url
                r = {
                    'type':'Reference',
                    'owner':'PEPFAR',
                    'owner_type':'Organization',
                    'collection':collection_id,
                    'data':{"expressions": ['/orgs/PEPFAR/sources/SIMS/concepts/' + concept_id + '/']}
                }
                output[url] = r
                num_references += 1
        ofile.write(json.dumps(output))

    if verbosity:
        print 'DHIS2 export successfully transformed and saved to "%s":' % (outputfile)
        print '\tConcepts: %s' % (num_concepts)
        print '\tReferences: %s' % (num_references)
        print '\tTOTAL: %s' % (num_concepts + num_references)
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
verbosity = 2   # 0 = none, 1 = some, 2 = all
old_dhis2_export_filename = 'old_sims_export.json'
new_dhis2_export_filename = 'new_sims_export.json'
converted_filename = 'converted_sims_export.json'
new_import_script = 'sims_import_script.json'
sims_collections_filename = 'ocl_sims_collections.json'
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
    #oclapitoken = 'a61ba53ed7b8b26ece8fcfc53022b645de0ec055'
    #oclenv = 'https://ocl-stg.openmrs.org'
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


# Offline settings (to run locally using cached versions of dhis2/ocl exports)
run_offline = True


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
    print '\tnew_import_script:', new_import_script
    print '\tsims_collections_filename:', sims_collections_filename
    if run_offline:
        print '\n**** RUNNING IN OFFLINE MODE ****'


# STEP 1: Fetch OCL Collections for SIMS Assessment Types
# Collections that have 'SIMS' in the name, __datim_sync==true, and external_id not empty
if verbosity:
    print '\n**** STEP 1 of 13: Fetch OCL Collections for SIMS Assessment Types'
if not run_offline:
    if verbosity:
        print 'Request URL:', url_sims_collections
    sims_collections = getRepositories(url=url_sims_collections, oclenv=oclenv,
                                       key_field='external_id', verbosity=verbosity)
    with open(attachAbsolutePath(sims_collections_filename), 'wb') as ofile:
        ofile.write(json.dumps(sims_collections))
    if verbosity:
        print 'Repositories retreived from OCL and stored in memory:', len(sims_collections)
        print 'Repositories successfully written to "%s"' % (sims_collections_filename)
else:
    if verbosity:
        print 'OFFLINE: Loading repositories from "%s"' % (sims_collections_filename)
    with open(attachAbsolutePath(sims_collections_filename), 'rb') as handle:
        sims_collections = json.load(handle)
    if verbosity:
        print 'OFFLINE: Repositories successfully loaded:', len(sims_collections)


# STEP 2: Extract list of DHIS2 dataset IDs from collection external_id
if verbosity:
    print '\n**** STEP 2 of 13: Extract list of DHIS2 dataset IDs from collection external_id'
str_active_dataset_ids = ','.join(sims_collections.keys())
if verbosity:
    print 'SIMS Assessment Type Dataset IDs:', str_active_dataset_ids


# STEP 3: Fetch new export from DATIM DHIS2
if verbosity:
    print '\n**** STEP 3 of 13: Fetch new export from DATIM DHIS2'
if not run_offline:
    content_length = saveDhis2SimsAssessmentTypesToFile(str_active_dataset_ids=str_active_dataset_ids,
                                                        filename=new_dhis2_export_filename,
                                                        dhis2env=dhis2env, dhis2uid=dhis2uid, dhis2pwd=dhis2pwd,
                                                        verbosity=verbosity)
    if verbosity:
        print '%s bytes retrieved from DHIS2 and written to file "%s"' % (content_length, new_dhis2_export_filename)
else:
    if verbosity:
        print 'OFFLINE: Using local file: "%s"' % (new_dhis2_export_filename)
    if os.path.isfile(attachAbsolutePath(new_dhis2_export_filename)):
        if verbosity:
            print 'OFFLINE: File "%s" found containing %s bytes. Continuing...' % (new_dhis2_export_filename, os.path.getsize(attachAbsolutePath(new_dhis2_export_filename)))
    else:
        print 'Could not find offline file "%s". Exiting...' % (new_dhis2_export_filename)
        exit(1)


# STEP 4: Quick comparison of current and previous DHIS2 exports
# Compares new DHIS2 export to most recent previous export from a successful sync that is available
# Comparison consists of file size check then file content comparison
# NOTE: This section should be skipped if doing the OCL/DHIS2 data validation check
if verbosity:
    print '\n**** STEP 4 of 13: Quick comparison of current and previous DHIS2 exports'
if not compare2previousexport:
    if verbosity: print "Skipping (due to settings)..."
elif not old_dhis2_export_filename:
    if verbosity: print "Skipping (no previous export filename provided)..."
else:
    if filecmp(old_dhis2_export_filename, new_dhis2_export_filename):
        print "Current and previous exports are identical, so exit without doing anything..."
        sys.exit()
    else:
        print "Current and previous exports are different in size and/or content, so continue..."


# STEP 5: Transform new DHIS2 export to OCL-formatted JSON (OJ)
# python dhis2oj-sims.py -i inputfile.xml -o outputfile.json -v1
if verbosity:
    print '\n**** STEP 5 of 13: Transform DHIS2 export to OCL formatted JSON'
dhis2oj_sims(inputfile=new_dhis2_export_filename, outputfile=converted_filename,
             sims_collections=sims_collections, verbosity=verbosity)


# STEP 6: Fetch latest versions of relevant OCL exports
if verbosity:
    print '\n**** STEP 6 of 13: Fetch latest versions of relevant OCL exports'
for k in ocl_export_defs:
    if verbosity:
        print '%s:' % (k)
    export_def = ocl_export_defs[k]
    if not run_offline:
        saveOclLatestExport(endpoint=export_def['endpoint'], tarfilename=export_def['tarfilename'],
                            jsonfilename=export_def['jsonfilename'], oclenv=oclenv, verbosity=verbosity)
    else:
        if verbosity:
            print '\tOFFLINE: Using local file "%s"...' % (export_def['jsonfilename'])
        if os.path.isfile(attachAbsolutePath(export_def['jsonfilename'])):
            if verbosity:
                print '\tOFFLINE: File "%s" found containing %s bytes. Continuing...' % (export_def['jsonfilename'], os.path.getsize(attachAbsolutePath(export_def['jsonfilename'])))
        else:
            print '\tCould not find offline file "%s". Exiting...' % (export_def['jsonfilename'])
            exit(1)


# STEP 7: Prepare OCL exports for diff
# Concepts/mappings in OCL exports have extra attributes that should be removed before the diff
# TODO: Clean OCL exports
if verbosity:
    print '\n**** STEP 7 of 13: Prepare OCL exports for diff'
for k in ocl_export_defs:
    if verbosity:
        print '%s:' % (k)
    with open(attachAbsolutePath(ocl_export_defs[k]['jsonfilename']), 'rb') as ifile, open(attachAbsolutePath(ocl_export_defs[k]['jsoncleanfilename']), 'wb') as ofile:
        ocl_sims_export = json.load(ifile)
        ocl_sims_export_clean = {}
        for c in ocl_sims_export['concepts']:
            # clean the concept and write it
            url = c['url']

            # Remove core fields
            core_fields_to_remove = ['version_created_by', 'created_on', 'updated_on', 'version_created_on', 'created_by', 'updated_by', 'display_name', 'display_locale', 'uuid', 'version', 'owner_url', 'source_url', 'mappings', 'url', 'version_url', 'is_latest_version', 'locale']
            for f in core_fields_to_remove:
                if f in c: del c[f]

            # Remove name fields
            name_fields_to_remove = ['uuid', 'type']
            if 'names' in c:
                for i, name in enumerate(c['names']):
                    for f in name_fields_to_remove:
                        if f in name: del name[f]

            ocl_sims_export_clean[url] = c
        for m in ocl_sims_export['mappings']:
            # clean the mapping and write it
            url = m['url']
            core_fields_to_remove = []
            for f in core_fields_to_remove:
                if f in m: del m[f]
            ocl_sims_export_clean[url] = m
        ofile.write(json.dumps(ocl_sims_export_clean))
        if verbosity:
            print '\tProcessed OCL export saved to "%s"' % (ocl_export_defs[k]['jsoncleanfilename'])


# STEP 8: Perform deep diff
# Note that multiple deep diffs may be performed, each with their own input and output files
if verbosity:
    print '\n**** STEP 8 of 13: Perform deep diff'
diff = None
with open(attachAbsolutePath(ocl_export_defs['sims_source']['jsoncleanfilename']), 'rb') as ocl_handle, open(attachAbsolutePath(converted_filename), 'rb') as dhis2_handle:
    a_ocl = json.load(ocl_handle)
    b_dhis2 = json.load(dhis2_handle)
    diff = DeepDiff(a_ocl, b_dhis2, ignore_order=True, verbose_level=2)
    if verbosity:
        print 'Diff results:'
        for k in diff:
            print '\t%s:' % (k), len(diff[k])


## STEP 9: Determine action based on diff result
# TODO: If data check only, then output need to return Success/Failure and then exit regardless
if verbosity:
    print '\n**** STEP 9 of 13: Determine action based on diff result'
if diff:
    print 'Deep diff identified one or more differences between DHIS2 and OCL...'
else:
    print 'No diff, exiting...'
    exit()


# STEP 10: Generate import scripts
# Generate import script by processing the diff results
# TODO: This currently only handles 'dictionary_item_added'
if verbosity:
    print '\n**** STEP 10 of 13: Generate import scripts'
with open(attachAbsolutePath(new_import_script), 'wb') as ofile:
    if 'dictionary_item_added' in diff:
        for k in diff['dictionary_item_added']:
            if 'type' in diff['dictionary_item_added'][k] and diff['dictionary_item_added'][k]['type'] == 'Concept':
                ofile.write(json.dumps(diff['dictionary_item_added'][k]))
                ofile.write('\n')
    if verbosity:
        print 'New import script written to file "%s"' % (new_import_script)


# STEP 11: Import the update script into ocl
# Parameters: testmode
#python oclimport.py -i importfile.json -v1 [--testmode=true] --ocltoken=... 1>oclimport-sims-stdout.log 2>oclimport-sims-stderr.log
if verbosity:
    print '\n**** STEP 11 of 13: Perform the import in OCL'


# STEP 12: Save new DHIS2 export for the next sync attempt
if verbosity:
    print '\n**** STEP 12 of 13: Save the DHIS2 export'


# STEP 13: Manage OCL repository versions
# create new version (maybe delete old version)
if verbosity:
    print '\n**** STEP 13 of 13: Manage OCL repository versions'

