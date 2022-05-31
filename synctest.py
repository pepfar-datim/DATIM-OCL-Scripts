"""
Script to test the synchronization by comparing the resulting metadata presentation
formats from DHIS2 and OCL.
"""
import settings
from datim import datimshow, datimsynctest


# OCL Settings - JetStream Staging user=datim-admin
oclenv = settings.oclenv
oclapitoken = settings.oclapitoken

# Perform the test and display results
datim_test = datimsynctest.DatimSyncTest(
    oclenv=oclenv, oclapitoken=oclapitoken, formats=[datimshow.DatimShow.DATIM_FORMAT_JSON])
datim_test.test_all()
