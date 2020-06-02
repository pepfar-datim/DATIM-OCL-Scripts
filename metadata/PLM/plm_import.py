import json
import ocldev.oclfleximporter

json_filename = 'plm_20191118_FP_expressions.json'
api_url_root = 'https://api.staging.openconceptlab.org'
api_token = 'c3b42623c04c87e266d12ae0e297abbce7f1cbe8'  # staging datim_admin
test_mode = False
importer = ocldev.oclfleximporter.OclFlexImporter(
	file_path=json_filename, api_url_root=api_url_root, api_token=api_token,
	test_mode=test_mode, verbosity=2, do_update_if_exists=True)
importer.process()
