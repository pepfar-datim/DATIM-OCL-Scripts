"""
Shared methods for performing ETL on DATIM metadata
"""

def get_as_csv(rows, start_columns=None, include_columns=None, exclude_columns=None,
               include_extras=True):
    """ Returm as CSV """
    if not rows:
        return ''
    if not isinstance(exclude_columns, list):
        exclude_columns = []

    # Build CSV column definition
    if include_columns:
        csv_columns = include_columns
    else:
        csv_columns = list(rows[0].keys())
        if 'extras' in csv_columns:
            csv_columns.remove('extras')

    # Add unique extra attributes to the CSV column definitions
    for row in rows:
        if 'extras' not in row:
            continue
        for attr_key in row['extras']:
            column_key = 'attr:%s' % attr_key
            if column_key not in csv_columns:
                if (include_columns and column_key in include_columns) or (not include_columns and column_key not in exclude_columns):
                    csv_columns.append('attr:%s' % attr_key)

    # Apply start columns to CSV column definitions
    if start_columns:
        count = 0
        for key in start_columns:
            if key in csv_columns:
                csv_columns.remove(key)
            csv_columns.insert(count, key)
            count += 1

    # Remove exclude columns from CSV column definitions
    if not include_columns and exclude_columns:
        for key in exclude_columns:
            if key in csv_columns:
                csv_columns.remove(key)

    # Generate the CSV output
    import csv
    import io
    output_stream = io.StringIO()
    writer = csv.DictWriter(output_stream, fieldnames=csv_columns)
    writer.writeheader()
    for row in rows:
        csv_row = {}
        for csv_column in csv_columns:
            if csv_column[:5] == 'attr:':
                extra_key = csv_column[5:]
                if 'extras' in row and extra_key in row['extras']:
                    csv_row[csv_column] = row['extras'][extra_key]
            else:
                csv_row[csv_column] = row.get(csv_column)
        writer.writerow(csv_row)
    return output_stream.getvalue().strip('\r\n')
