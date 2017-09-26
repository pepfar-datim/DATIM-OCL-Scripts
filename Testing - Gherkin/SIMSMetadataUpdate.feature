Feature: Indicator Metadata Update
  This test is designed to prove that SIMS updates in DHIS2 are propigated to OCL and that data matches 

Scenario: 
	Given That a user logs into DHIS2
		And adds a SIMS data element  
		And updates another SIMS data element's name
		And updates another SIMS data element's short name
	When SIMS metadata sync is triggered
		Then the metadata sync successfully completes 
        And the new SIMS data element is present in OCL 
        And the SIMS data elements that were updated are present in OCL
        And the OCL system and the DHIS2 system have the same SIMS data when compared
