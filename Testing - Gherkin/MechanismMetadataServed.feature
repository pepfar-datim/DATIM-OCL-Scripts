Feature: Mechanism Metadata Served 
  This test is designed to prove that the OCL mechanism metadata requests provide mechanism metadata that matches the  DATIM mechanism Requests
  End-users may request to get HTML, JSON, CSV or XML version of the DATIM Mechanism Metadata and it successfully produces the same resut from OCL as one would get from DATIM.  

Scenario: 
	Given That the OCL system has been populated with Datim mechanism metadata
		And OCL system is being regreshed with updated mechanism metadta
	Given that the end-user has navigated to the OpenHIE metadata page
		And the end-user desires mechanism metadata to be delivered 
		And the end-user has determined which type of metadata that is desired
	When the end-user clicks on the metadata link for <OCLMechanismData>
		Then the <OCLMechanismData> matches  the result when the end user clicks on the link for <PEPFARMechanismData>
		Examples:
    |OCLMechanismData|PEPFARMechanismData|
    |https://www.datim.org/api/sqlViews/fgUtV6e9YIX/data.html+css||
    |https://www.datim.org/api/sqlViews/fgUtV6e9YIX/data.json||
    |https://www.datim.org/api/sqlViews/fgUtV6e9YIX/data.csv||
    |https://www.datim.org/api/sqlViews/fgUtV6e9YIX/data.xml||