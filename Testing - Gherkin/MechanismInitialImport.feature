Feature: Mechanism Metadata Update
  This test is designed to verify the initial import 

Scenario: 
	Given That DHIS2 has mechanism data
		And that OCL Mechanism data is currently not loaded 
	When mechanism metadata sync  to OCL is triggered
		Then the metadata sync successfully completes 
        And the mechanism metadata is present in OCL
        And the OCL system and the DHIS2 system have the same mechanism data when compared
