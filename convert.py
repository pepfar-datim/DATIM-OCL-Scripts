import json
import argparse


new_country_names = {
    "BWA": "Botswana", # "BW"
    "BDI": "Burundi",  # BI
    "CMR": "Cameroon",  # CM
    "CIV": "Cote d'Ivoire",  # CI
    "DEMO": "demo_country",  # DEMO
    "COD": "Democratic Republic of the Congo",  # CD
    "SWZ": "Eswatini",  # SZ
    "ETH": "Ethiopia",  # ET
    "HTI": "Haiti",  # HT
    "KEN": "Kenya",  # KE
    "LSO": "Lesotho",  # LS
    "MWI": "Malawi",  # MW
    "MOZ": "Mozambique",  # MZ
    "NAM": "Namibia",  # NA
    "NGA": "Nigeria",  # NG
    "RWA": "Rwanda",  # RW
    "ZAF": "South Africa",  # ZA
    "SSD": "South Sudan",  # SS
    "TZA": "Tanzania",  # TZ
    "UGA": "Uganda",  # UG
    "UKR": "Ukraine",  # UA
    "VNM": "Vietnam",  # VN
    "ZMB": "Zambia",  # ZM
    "ZWE": "Zimbabwe",  # ZW
}
old_country_names = {
    "BW": "Botswana",
    "BI": "Burundi",
    "CM": "Cameroon",
    "CI": "Cote d'Ivoire",
    "CD": "Democratic Republic of the Congo",
    "SZ": "Eswatini",
    "ET": "Ethiopia",
    "HT": "Haiti",
    "KE": "Kenya",
    "LS": "Lesotho",
    "MW": "Malawi",
    "MZ": "Mozambique",
    "NA": "Namibia",
    "NG": "Nigeria",
    "RW": "Rwanda",
    "ZA": "South Africa",
    "SS": "South Sudan",
    "TZ": "Tanzania",
    "UG": "Uganda",
    "UA": "Ukraine",
    "VN": "Vietnam",
    "ZM": "Zambia",
    "ZW": "Zimbabwe",
}



parser = argparse.ArgumentParser(
    "imap-restore", description="Restore IMAP backup file to OCL environment")
parser.add_argument(
    'file', type=argparse.FileType('r'), help='IMAP file (JSON or CSV), eg "BI-FY20.csv"')
parser.add_argument(
    '-v', '--verbosity', help='Verbosity level: 0 (default), 1, or 2', default=0, type=int)
args = parser.parse_args()
imap_backups = json.load(args.file)
if args.verbosity:
    print args
    print '%s IMAP backups loaded' % len(imap_backups)

current_num = 0
for imap_backup in imap_backups:
    current_num += 1

    # Display debug info for the current IMAP org
    imap_backup['old_country_code'] = imap_backup['country_code']
    imap_backup['old_country_org'] = imap_backup['country_org']
    imap_backup['country_code'] = new_country_names.keys()[
        new_country_names.values().index(imap_backup['country_name'])]
    imap_backup['country_org'] = 'DATIM-MOH-%s-%s' % (
        imap_backup['country_code'], imap_backup['period'])
    if args.verbosity:
        print '** [IMAP %s of %s] Org: %s, Country Code: %s, Country Name: %s, Period: %s' % (
            current_num, len(imap_backups), imap_backup['old_country_org'],
            imap_backup['old_country_code'], imap_backup['country_name'], imap_backup['period'])
        print '              New Org: %s, Country-Code: %s' % (
            imap_backup['country_org'], imap_backup['country_code'])

print json.dumps(imap_backups)

# country_name = args.country_name
# if not country_name and args.country_code in country_names:
#     country_name = country_names[args.country_code]
# if not country_name:
#     country_name = args.country_code