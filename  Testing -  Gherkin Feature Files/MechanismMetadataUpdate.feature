Feature: Mechanism Metadata Update
  This test is designed to prove that mechanism updates in DHIS2 are propigated to OCL and that data matches 

Scenario: Mechanism metadata has been updated in DHIS2 and changes need to be synced to OCL
	Given That a user logs into DHIS2
		And adds a mechanism (category option)
		And updates another mechanism's data name
		And updates another mechanism's short name
	When mechanism metadata sync  to OCL is triggered
		Then the metadata sync successfully completes 
        And the new mechanism (category option) is present in OCL 
        And the mechanism (category option) that were updated are present in OCL
        And the OCL system and the DHIS2 system have the same mechanism data when compared

Scenario: mechanism metadata has not been updated, but the sync process runs
    Given That OCL and DHIS2 are already synchronized
    When mechanism metadata sync is triggered
        Then the metadata sync successfully completes 
        And the OCL system and the DHIS2 system have the same mechanism data when compared