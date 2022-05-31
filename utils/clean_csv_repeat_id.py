"""
Use this script to 'clean' an IMAP CSV file before import.
This was required for several of the 2018 IMAP files.
If MOH_Indicator_ID == MOH_Disag_ID, this is the equivalent of there being no
disag, so this script simply sets the MOH_Disag_ID and MOH_Disag_Name fields
to empty strings.

NOTE:
"""

import csv
import sys

from datim import datimimap

verbosity = 0
csv_filename = "csv/KE-FY18-dirty.csv"

# Load the IMAP CSV
imap = datimimap.DatimImapFactory.load_imap_from_csv(
    csv_filename=csv_filename, period="",
    country_org="", country_name="", country_code="")
if verbosity >= 2:
    imap.display(fmt='csv', sort=True, exclude_empty_maps=True, auto_fix_null_disag=True)

# Output a cleaned version
count = 0
writer = csv.DictWriter(sys.stdout, fieldnames=datimimap.DatimImap.IMAP_EXPORT_FIELD_NAMES)
writer.writeheader()
for row_number in range(imap.length()):
    row = imap.get_row(row_number)
    if row['MOH_Indicator_ID'] and row['MOH_Indicator_ID'] == row['MOH_Disag_ID']:
        count += 1
        row['MOH_Disag_ID'] = ''
        row['MOH_Disag_Name'] = ''
        if verbosity >= 2:
            print('Matching IDs: %s  %s' % (row['MOH_Indicator_ID'], row))
    writer.writerow(row)
if verbosity >= 1:
    print('\n** %s reused ID(s)\n' % count)
