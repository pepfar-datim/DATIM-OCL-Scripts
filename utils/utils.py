import json
import requests
import oclfleximporter
import time


# JetStream staging
oclenv = 'https://api.staging.openconceptlab.org'
oclapitoken = 'c3b42623c04c87e266d12ae0e297abbce7f1cbe8'
endpoint = '/orgs/PEPFAR/collections/'
oclapiheaders = {
    'Authorization': 'Token ' + oclapitoken,
    'Content-Type': 'application/json'
}
url_all_collections = oclenv + endpoint + '?limit=100'
# url = oclenv + endpoint + '?q=MER+Results+Facility+dod&verbose=true&limit=100'
r = requests.get(url_all_collections, headers=oclapiheaders)
collections = r.json()


# Set custom attribute
attr_name = 'datim_sync_mer'
attr_value = True
def set_extra_attr(repo, attr_name='', attr_value=''):
    repo_url = oclenv + repo['url']
    repo['extras'][attr_name] = attr_value
    new_attr_data = {'extras': repo['extras']}
    print repo_url, '\n', json.dumps(new_attr_data)
    r = requests.put(repo_url, data=json.dumps(new_attr_data), headers=oclapiheaders)
    print r.status_code, r.text

def set_repo_field(repo_endpoint, field, value):
    repo_url = oclenv + repo_endpoint
    new_data = {field: value}
    print repo_url, '\n', json.dumps(new_data)
    r = requests.put(repo_url, data=json.dumps(new_data), headers=oclapiheaders)
    print r.status_code, r.text

# Delete matching repos
for c in collections:
    if c['id'].find('SIMS') == 0 or c['id'].find('Tiered') == 0:
        continue
    delete_request = requests.delete(oclenv + c['url'], headers=oclapiheaders)
    print 'DELETE', c['id'], 'STATUS CODE:', delete_request.status_code

def create_initial_repo_version(collections):
    missing_count = 0
    all_good_count = 0
    for c in collections:
        latest_url = oclenv + c['url'] + 'latest/'
        r = requests.get(latest_url, headers=oclapiheaders)
        if r.status_code == 200:
            print "ALL GOOD:", latest_url
            all_good_count += 1
        elif r.status_code == 404:
            missing_count += 1
            print "MISSING: need initial version for '%s'" % c['url']
            create_initial_repo_version(oclenv + c['url'], oclapiheaders=oclapiheaders)

def create_missing_exports(collections):
    for c in collections:
        latest_url = oclenv + c['url'] + 'latest/'
        latest_response = requests.get(latest_url, headers=oclapiheaders)
        if latest_response.status_code != 200:
            print 'SKIPPING: No latest version defined for repo "%s"' % c['url']
            continue
        latest_repo_version = latest_response.json()
        # try to get the export
        export_url = oclenv + latest_repo_version['version_url'] + 'export/'
        get_export_response = requests.get(export_url, headers=oclapiheaders)
        print 'GET %s: STATUS CODE %s' % (export_url, get_export_response.status_code)
        if get_export_response.status_code == 204:
            # create the export
            post_export_response = requests.post(export_url, headers=oclapiheaders)
            print 'POST %s: STATUS CODE %s' % (export_url, post_export_response.status_code)


def create_repo_version(repo_url='', oclapiheaders=None, version_desc='Automatically generated initial empty repository version', version_id='initial', released=True):
    """ Create new repo version """
    new_version_url = repo_url + 'versions/'
    new_repo_version_data = {
        'id': version_id,
        'description': version_desc,
        'released': released,
    }
    print 'POST %s: %s' % (new_version_url, str(new_repo_version_data))
    r = requests.post(new_version_url, headers=oclapiheaders, data=json.dumps(new_repo_version_data))
    print 'STATUS CODE:', r.status_code
    r.raise_for_status()


mer = []
for c in collections:
    if c['id'].find('SIMS') == 0 or c['id'].find('Tiered') == 0:
        continue
    mer.append(c)
    print c['id']

for c in collections:
    add_attr(c, attr_name=attr_name, attr_value=attr_value)

for c in collections:
    if 'extras' in c and 'datim_sync_mer' in c['extras']:
        print c['id']

for c in collections:
    if c['id'].find('SIMS') == 0:
        if c['id'] not in sims_external_ids:
            continue
        print c['url'], 'external_id =', sims_external_ids[c['id']]
        set_repo_field(c['url'], 'external_id', sims_external_ids[c['id']])

        print c['id'], '[', c['external_id'], ']:', c['extras']
        add_attr(c, attr_name='datim', attr_value=attr_value)

sims_external_ids = {
    'SIMS3-Facility': 'FZxMe3kfzYo',
    'SIMS3-Community': 'jvuGqgCvtcn',
    'SIMS3-Above-Site': 'OuWns35QmHl',
    'SIMS2-Facility': 'Run7vUuLlxd',
    'SIMS2-Community': 'xGbB5HNDnC0',
    'SIMS2-Above-Site': 'xDFgyFbegjl'
}

# Create new repo version if only 'initial' empty version exists
r = requests.get(url_all_collections, headers=oclapiheaders)
collections = r.json()
cnt = 0
for c in collections:
    if c['id'].find('SIMS') == 0 or c['id'].find('Tiered') == 0:
        continue
    latest_version_request = requests.get(oclenv + c['url'] + 'latest/', headers=oclapiheaders)
    latest_version_json = latest_version_request.json()
    if latest_version_json['id'] == 'initial':
        cnt += 1
        print '[%s] Creating new version for "%s"' % (cnt, c['id'])
        try:
            create_repo_version(repo_url=oclenv + c['url'], oclapiheaders=oclapiheaders, version_desc='Automatically generated repository version', version_id='v2017-10-02', released=True)
        except:
            print "That one failed... but no way we're going to let that keep us donw..."
        print 'Sleeping for 10 seconds...'
        time.sleep(10)



for c in collections:
    if c['id'].find('SIMS') == 0 or c['id'].find('Tiered') == 0:
        continue
    latest_version_request = requests.get(oclenv + c['url'] + 'latest/', headers=oclapiheaders)
    latest_version_json = latest_version_request.json()
    if latest_version_json['id'] == 'initial':

# import_filename = 'init/temp.json'
import_filename = 'mer_dhis2ocl_import_script.json'
importer_collections = oclfleximporter.OclFlexImporter(
    file_path=import_filename, limit=1, api_url_root=oclenv, api_token=oclapitoken, test_mode=False)
importer_collections.process()
