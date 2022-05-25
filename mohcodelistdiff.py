import csv
from deepdiff import DeepDiff

# FY18: sfk9cyQSUyi
# FY19: OBhi1PUW3OL
# FY20: QSodwF4YG9a

# periods = ['FY18', 'FY19', 'FY20']
periods = ['FY19', 'FY20']

for period in periods:
    ocl_filename = 'ocl_moh_%s.csv' % period
    dhis2_filename = 'dhis2_moh_%s.csv' % period

    ocl_codelist = {}
    with open(ocl_filename) as ocl_file:
        ocl_reader = csv.DictReader(ocl_file)
        for row in ocl_reader:
            row_id = '%s.%s' % (row['dataelementuid'], row['categoryoptioncombouid'])
            ocl_codelist[row_id] = row
            # ocl_codelist.append(row)

    dhis2_codelist = {}
    with open(dhis2_filename) as dhis2_file:
        dhis2_reader = csv.DictReader(dhis2_file)
        for row in dhis2_reader:
            row_id = '%s.%s' % (row['dataelementuid'], row['categoryoptioncombouid'])
            dhis2_codelist[row_id] = row
            # dhis2_codelist.append(row)

    # Sort lists
    # ocl_codelist_sorted = sorted(ocl_codelist, key = lambda i: (i['id']))
    # dhis2_codelist_sorted = sorted(dhis2_codelist, key = lambda i: (i['id']))
    # ddiff = DeepDiff(dhis2_codelist_sorted, ocl_codelist_sorted)

    ddiff = DeepDiff(dhis2_codelist, ocl_codelist)
    print('%s:' % period)
    print(ddiff)
    print('\n\n')

    # print period, len(ocl_codelist), len(dhis2_codelist)
