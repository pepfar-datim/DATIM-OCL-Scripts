"""
Shared methods for performing ETL on DATIM metadata
"""

def get_as_csv(rows, start_columns=None, exclude_columns=None, include_extras=True):
    if not rows:
        return ''
    csv_columns = rows[0].keys()
    if 'extras' in csv_columns:
        csv_columns.remove('extras')

    # Apply start columns
    if start_columns:
        count = 0
        for key in start_columns:
            if key in csv_columns:
                csv_columns.remove(key)
            csv_columns.insert(count, key)
            count += 1

    # Apply exclude columns
    if exclude_columns:
        for key in exclude_columns:
            if key in csv_columns:
                csv_columns.remove(key)

    # Add unique extra attribute columns
    for row in rows:
        if 'extras' not in row:
            continue
        for attr_key in row['extras']:
            if ('attr:%s' % attr_key) not in csv_columns:
                csv_columns.append('attr:%s' % attr_key)

    # Generate the CSV output
    import csv
    import io
    output_stream = io.BytesIO()
    writer = csv.DictWriter(output_stream, fieldnames=csv_columns)
    writer.writeheader()
    for row in rows:
        csv_row = row.copy()
        if exclude_columns:
            for key in exclude_columns:
                if key in csv_row:
                    del csv_row[key]
        if 'extras' in csv_row:
            del csv_row['extras']
            if include_extras:
                for attr_key in row['extras']:
                    csv_row['attr:%s' % attr_key] = row['extras'][attr_key]
        writer.writerow(csv_row)
    return output_stream.getvalue().strip('\r\n')
