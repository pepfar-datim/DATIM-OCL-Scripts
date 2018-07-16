"""
Script to test the synchronization by comparing the resulting metadata presentation
formats from DHIS2 and OCL.
"""
import settings
import datim.datimshow
import datim.datimsynctest


# OCL Settings - JetStream Staging user=datim-admin
oclenv = settings.ocl_api_url_staging
oclapitoken = settings.api_token_staging_datim_admin

# Perform the test and display results
datim_test = datim.datimsynctest.DatimSyncTest(
	oclenv=oclenv, oclapitoken=oclapitoken,formats=[datim.datimshow.DatimShow.DATIM_FORMAT_JSON])
datim_test.test_all()
