"""
Script to import a country mapping CSV for a specified country (e.g. UG) and
period (e.g. FY17). CSV must follow the format of the country mapping CSV template.
"""
import settings
from datim.datimimapexport import DatimImapExport
from datim.datimimapexport import DatimImapFactory


# Set attributes
csv_filename = 'imap.csv'
country_code = 'UG'
period = 'FY17'

# Load i-map from CSV file
imap_csv = DatimImapFactory.load_imap_from_csv(csv_filename)
if not imap_csv:
	raise Exception("Unable to load i-map from CSV from filename '%s'" % (csv_filename))

# Load i-map from OCL using country code and period
imap_ocl = DatimImapFactory.load_imap_from_ocl(country_code, period)
if not imap_ocl:
	raise Exception("Unable to load i-map from OCL for country code '%s' and period" % (country_code, period))

# Compare i-map in the CSV and OCL and generate an OCL import script
imap_diff = DatimImapFactory.compare(imap_csv, imap_ocl)
imap_import_script = DatimImapFactory.get_ocl_import_script_from_diff(imap_diff)

# Now import the changes


# Set the repository versions

