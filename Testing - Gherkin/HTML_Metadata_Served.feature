Feature: Metadata Served in HTML
  End-users may request to get HTML version of the DATIM Metadata 

Scenario: 
	Given That the OCL system has been populated with Datim metadata
		And OCL system is being regreshed with updated metadta
	Given that the end-user has navigated to the OpenHIE metadata page
		And the end-user desires metadata to be delivered in HTML
		And the end-user has determined which type of metadata that is desired
	When the end-user clicks on <metadata link>
		Then the <OCL HTML Result> matches <PEPFAR HTML Result>
		Examples:
    | Metadata Link | OCL HTML Result | PEPFAR HTML Result |
    |    |    |     |
    |    |    |    | 