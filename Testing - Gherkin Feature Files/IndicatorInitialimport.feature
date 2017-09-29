Feature: Indicator Metadata Initial import
  This test is designed to verify the initial indicator metadata import 

Scenario: 
	Given That DHIS2 has indicator metadata
		And that OCL indicator metadata is currently not loaded 
	When indicator metadata sync  to OCL is triggered
		Then the metadata sync successfully completes 
        And the indicator metadata is present in OCL
        And the OCL system and the DHIS2 system have the same indicator metadatadata when compared
