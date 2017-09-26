Feature: SIMS Metadata Served 
  This test is designed to prove that the OCL SIMS metadata requests provide SIMS metadata that matches the  DATIM SIMS Requests
  End-users may request to get HTML, JSON, CSV or XML version of the DATIM SIMS Metadata and it successfully produces the same resut from OCL as one would get from DATIM.  

Scenario: 
	Given That the OCL system has been populated with Datim SIMS metadata
		And OCL system is being regreshed with updated SIMS metadta
	Given that the end-user has navigated to the OpenHIE metadata page
		And the end-user desires SIMS metadata to be delivered 
		And the end-user has determined which type of metadata that is desired
	When the end-user clicks on the metadata link for <OCLSIMSData>
		Then the <OCLSIMSData> matches  the result when the end user clicks on the link for <PEPFARSIMSData>
		Examples:
    |OCLSIMSData|PEPFARSIMSData|
  	|https://dev-de.datim.org/api/sqlViews/uMvWjOo31wt/data.html+css||
  	|https://dev-de.datim.org/api/sqlViews/uMvWjOo31wt/data.json||
  	|https://dev-de.datim.org/api/sqlViews/uMvWjOo31wt/data.csv||
  	|https://dev-de.datim.org/api/sqlViews/uMvWjOo31wt/data.xml||
  	|https://dev-de.datim.org/api/sqlViews/PB2eHiURtwS/data.html+css||
  	|https://dev-de.datim.org/api/sqlViews/PB2eHiURtwS/data.json||
  	|https://dev-de.datim.org/api/sqlViews/PB2eHiURtwS/data.csv||
  	|https://dev-de.datim.org/api/sqlViews/PB2eHiURtwS/data.xml||
  	|https://dev-de.datim.org/api/sqlViews/wL1TY929jCS/data.html+css||
  	|https://dev-de.datim.org/api/sqlViews/wL1TY929jCS/data.json||
  	|https://dev-de.datim.org/api/sqlViews/wL1TY929jCS/data.csv||
  	|https://dev-de.datim.org/api/sqlViews/wL1TY929jCS/data.xml||
  	|https://dev-de.datim.org/api/sqlViews/JlRJO4gqiu7/data.html+css||
  	|https://dev-de.datim.org/api/sqlViews/JlRJO4gqiu7/data.json||
  	|https://dev-de.datim.org/api/sqlViews/JlRJO4gqiu7/data.csv||
  	|https://dev-de.datim.org/api/sqlViews/JlRJO4gqiu7/data.xml||