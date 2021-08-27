"""
Use this script to test one or multiple IMAP CSV files for validity and
that the DatimImap object is interpreting the values correctly.
"""
import datim.datimimap

verbosity = 1
country_codes = {
        ### "BW": "Botswana",
        "BI": "Burundi",
        ### "CM": "Cameroon",
        "CI": "Cote d'Ivoire",
        "CD": "Democratic Republic of the Congo",
        ### "SZ": "Eswatini",
        ### "ET": "Ethiopia",
        ### "HT": "Haiti",
        "KE": "Kenya",
        "LS": "Lesotho",
        "MW": "Malawi",
        "MZ": "Mozambique",
        "NA": "Namibia",
        ### "NG": "Nigeria",
        ### "RW": "Rwanda",
        "ZA": "South Africa",
        "SS": "South Sudan",
        "TZ": "Tanzania",
        "UG": "Uganda",
        "UA": "Ukraine",
        "VN": "Vietnam",
        "ZM": "Zambia",
        "ZW": "Zimbabwe",
    }

# Iterate through IMAPs and test
period = 'FY18'
for country_code in sorted(country_codes.iterkeys()):
    csv_filename = 'csv/%s-%s.csv' % (country_code, period)
    country_name = country_codes[country_code]
    country_org = 'DATIM-MOH-%s' % country_code
    if verbosity >= 1:
        print '\n\n*****************************************************************************'
        print '**  %s, %s, %s' % (country_code, country_name, country_org)
        print '*****************************************************************************'

    # Load the IMAP CSV
    imap = datim.datimimap.DatimImapFactory.load_imap_from_csv(
        csv_filename=csv_filename, period=period,
        country_org=country_org, country_name=country_name, country_code=country_code)
    imap.do_add_columns_to_csv = False
    if verbosity >= 2:
        imap.display(fmt='csv', sort=True, exclude_empty_maps=True, auto_fix_null_disag=True)
        print ''

    # Having some fun
    """
    print imap.has_country_disag(disag_id='PXW9InWF1FD', disag_name="<2 Years, Females")
    imap.do_add_columns_to_csv = True
    for row in imap:
        print row
    exit()
    row_num = 20
    print(imap.get_row(row_num, include_extra_info=False, auto_fix_null_disag=False, convert_to_dict=False, exclude_empty_maps=False))
    print(imap.get_row(row_num, include_extra_info=True, auto_fix_null_disag=False, convert_to_dict=False, exclude_empty_maps=False))
    print(imap.get_row(row_num, include_extra_info=False, auto_fix_null_disag=True, convert_to_dict=False, exclude_empty_maps=False))
    print(imap.get_row(row_num, include_extra_info=True, auto_fix_null_disag=True, convert_to_dict=False, exclude_empty_maps=False))
    print(imap.get_row(row_num, include_extra_info=False, auto_fix_null_disag=False, convert_to_dict=True, exclude_empty_maps=False))
    print(imap.get_row(row_num, include_extra_info=True, auto_fix_null_disag=False, convert_to_dict=True, exclude_empty_maps=False))
    print(imap.get_row(row_num, include_extra_info=False, auto_fix_null_disag=True, convert_to_dict=True, exclude_empty_maps=False))
    print(imap.get_row(row_num, include_extra_info=True, auto_fix_null_disag=True, convert_to_dict=True, exclude_empty_maps=False))
    print(imap.get_row(row_num, include_extra_info=False, auto_fix_null_disag=False, convert_to_dict=False, exclude_empty_maps=True))
    print(imap.get_row(row_num, include_extra_info=True, auto_fix_null_disag=False, convert_to_dict=False, exclude_empty_maps=True))
    print(imap.get_row(row_num, include_extra_info=False, auto_fix_null_disag=True, convert_to_dict=False, exclude_empty_maps=True))
    print(imap.get_row(row_num, include_extra_info=True, auto_fix_null_disag=True, convert_to_dict=False, exclude_empty_maps=True))
    print(imap.get_row(row_num, include_extra_info=False, auto_fix_null_disag=False, convert_to_dict=True, exclude_empty_maps=True))
    print(imap.get_row(row_num, include_extra_info=True, auto_fix_null_disag=False, convert_to_dict=True, exclude_empty_maps=True))
    print(imap.get_row(row_num, include_extra_info=False, auto_fix_null_disag=True, convert_to_dict=True, exclude_empty_maps=True))
    print(imap.get_row(row_num, include_extra_info=True, auto_fix_null_disag=True, convert_to_dict=True, exclude_empty_maps=True))
    exit()
    """

    # Test 1: Count of reused MOH Indicator ID in the Disag ID column
    count = 0
    for row in imap.get_imap_data():
        if row['MOH_Indicator_ID'] and row['MOH_Indicator_ID'] == row['MOH_Disag_ID']:
            count += 1
            if verbosity >= 2:
                print('Matching IDs: %s == %s  %s' % (row['MOH_Indicator_ID'], row['MOH_Disag_ID'], row))
    if verbosity >= 1:
        print('%s (%s): %s reused ID(s)' % (country_code, country_name, count))

    # Test 2a: Blank Disag ID count
    count = 0
    for row in imap.get_imap_data(auto_fix_null_disag=False):
        if row['MOH_Indicator_ID'] and not row['MOH_Disag_ID']:
            count += 1
    if verbosity >= 1:
        print('%s (%s): %s blank Disag ID(s)' % (country_code, country_name, count))

    # Test 2b: After applying the fix
    county = 0
    for row in imap.get_imap_data(auto_fix_null_disag=True):
        if row['MOH_Indicator_ID'] and not row['MOH_Disag_ID']:
            count += 1
    if verbosity >= 1:
        print('%s (%s): %s blank Disag ID(s) ... AFTER FIXING' % (country_code, country_name, count))

    # Test 3: Reused IDs with different names
    tracker_moh_indicator = {}
    tracker_moh_disag = {}
    for row in imap:
        if row['MOH_Indicator_ID']:
            if row['MOH_Indicator_ID'] not in tracker_moh_indicator:
                tracker_moh_indicator[row['MOH_Indicator_ID']] = {}
            if row['MOH_Indicator_Name'] not in tracker_moh_indicator[row['MOH_Indicator_ID']]:
                tracker_moh_indicator[row['MOH_Indicator_ID']][row['MOH_Indicator_Name']] = 0
            tracker_moh_indicator[row['MOH_Indicator_ID']][row['MOH_Indicator_Name']] += 1
        if row['MOH_Disag_ID']:
            if row['MOH_Disag_ID'] not in tracker_moh_disag:
                tracker_moh_disag[row['MOH_Disag_ID']] = {}
            if row['MOH_Disag_Name'] not in tracker_moh_disag[row['MOH_Disag_ID']]:
                tracker_moh_disag[row['MOH_Disag_ID']][row['MOH_Disag_Name']] = 0
            tracker_moh_disag[row['MOH_Disag_ID']][row['MOH_Disag_Name']] += 1
    if verbosity >= 2:
        print('** MOH Indicator Name Counts')
    num_replaced_indicator_names = 0
    for resource_id in tracker_moh_indicator:
        if len(tracker_moh_indicator[resource_id]) > 1:
            num_replaced_indicator_names += len(tracker_moh_indicator[resource_id]) - 1
            if verbosity >= 2:
                print resource_id, tracker_moh_indicator[resource_id]
    print(tracker_moh_indicator)
    if verbosity >= 2:
        print '** MOH Disag Name Counts'
    num_replaced_disag_names = 0
    for resource_id in tracker_moh_disag:
        if len(tracker_moh_disag[resource_id]) > 1:
            num_replaced_disag_names += len(tracker_moh_disag[resource_id]) - 1
            if verbosity >= 2:
                print resource_id, tracker_moh_disag[resource_id]
    print(tracker_moh_disag)
    if verbosity >= 1:
        print '%s (%s): %s replaced Indicator Names; %s replaced Disag Names' % (
            country_code, country_name, num_replaced_indicator_names, num_replaced_disag_names)
