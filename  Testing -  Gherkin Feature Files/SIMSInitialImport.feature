Feature: SIMS Metadata Initial import
  This test is designed to verify the initial SIMS metadata import 

Scenario: 
	Given That DHIS2 has SIMS metadata
		And that OCL SIMS metadata is currently not loaded 
	When SIMS metadata sync  to OCL is triggered
		Then the metadata sync successfully completes 
        And the SIMS metadata is present in OCL
        And the OCL system and the DHIS2 system have the same SIMS metadatadata when compared
