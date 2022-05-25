import json
import ocldev.oclcsvtojsonconverter
import ocldev.oclfleximporter

# Input parameters
csv_filename = 'plm_20191118.csv'  # Use this if loading from file
# input_list = []  # Use this if pre-loaded as a list of Python dictionaries, where each key/value pair is a cell in the CSV
verbose = False
csv_processing_method = ocldev.oclcsvtojsonconverter.OclCsvToJsonConverter.PROCESS_BY_DEFINITION  # By definition is default. Or PROCESS_BY_ROW

# Process the CSV -- Use the first one if loading from CSV file or the 2nd one if it's already loaded into the input_list variable
csv_converter = ocldev.oclcsvtojsonconverter.OclStandardCsvToJsonConverter(csv_filename=csv_filename, verbose=verbose)
# csv_converter = oclcsvtojsonconverter.OclStandardCsvToJsonConverter(input_list=input_list, verbose=verbose)
if csv_processing_method == ocldev.oclcsvtojsonconverter.OclCsvToJsonConverter.PROCESS_BY_ROW:
    results = csv_converter.process_by_row()
else:
    results = csv_converter.process_by_definition()

# Output as response -- note that output happens during processing if verbose is True
if not verbose:
    for result in results:
        print((json.dumps(result)))
