Feature: Indicator Metadata Update
  This test is designed to prove that SIMS updates in DHIS2 are propigated to OCL and that data matches 

Scenario: SIMS metadata has been updated in DHIS2 and needs to be synced to OCL 
	Given That a user logs into DHIS2
		And adds a SIMS data element  
		And updates another SIMS data element's name
		And updates another SIMS data element's short name
	When SIMS metadata sync is triggered
		Then the metadata sync successfully completes 
        And the new SIMS data element is present in OCL 
        And the SIMS data elements that were updated are present in OCL
        And the OCL system and the DHIS2 system have the same SIMS data when compared

Scenario: SIMS metadata has not been updated, but the sync process runs
    Given That OCL and DHIS2 are already synchronized
    When SIMS metadata sync is triggered
        Then the metadata sync successfully completes 
        And the OCL system and the DHIS2 system have the same SIMS data when compared