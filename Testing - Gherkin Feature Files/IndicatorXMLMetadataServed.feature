Feature: Indicator Metadata Served in xml
  This test is designed to prove that the OCL indicator xml Requests provide metadata that matches the indicator DATIM xml Requests
  End-users may request to get xml version of the DATIM Indicator Metadata and it successfully produces the same resut from OCL as one would get from DATIM.  

Scenario: 
	Given That the OCL system has been populated with Datim metadata
		And OCL system is being regreshed with updated metadta
	Given that the end-user has navigated to the OpenHIE metadata page
		And the end-user desires metadata to be delivered in xml
		And the end-user has determined which type of metadata that is desired
	When the end-user clicks on OCL xml metadata link for <OCLxmlResult>
		Then the <OCLxmlResult> matches  the result when the end user clicks on <PEPFARxmlResult>
 	Examples:
    |OCLxmlResult|PEPFARxmlResult|
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.xml?var=dataSets:kkXf2zXqTM0||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.xml?var=dataSets:MqNLEXmzIzr||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.xml?var=dataSets:K7FMzevlBAp||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.xml?var=dataSets:UZ2PLqSe5Ri||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.xml?var=dataSets:CGoi5wjLHDy||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.xml?var=dataSets:LWE9GdlygD5||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.xml?var=dataSets:tG2hjDIaYQD||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.xml?var=dataSets:Kxfk0KVsxDn||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.xml?var=dataSets:hgOW2BSUDaN||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.xml?var=dataSets:Awq346fnVLV||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.xml?var=dataSets:Awq346fnVLV||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.xml?var=dataSets:ovmC3HNi4LN||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.xml?var=dataSets:CGoi5wjLHDy||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.xml?var=dataSets:zTgQ3MvHYtk||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.xml?var=dataSets:KwkuZhKulqs||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.xml?var=dataSets:eAlxMKMZ9GV||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.xml?var=dataSets:PkmLpkrPdQG||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.xml?var=dataSets:zeUCqFKIBDD||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.xml?var=dataSets:AitXBHsC7RA||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.xml?var=dataSets:BuRoS9i851o||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.xml?var=dataSets:jEzgpBt5Icf||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.xml?var=dataSets:ePndtmDbOJj||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.xml?var=dataSets:AvmGbcurn4K||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.xml?var=dataSets:O8hSwgCbepv||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.xml?var=dataSets:bqiB5G6qgzn||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.xml?var=dataSets:YWZrOj5KS1c||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.xml?var=dataSets:c7Gwzm5w9DE||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.xml?var=dataSets:pTuDWXzkAkJ||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.xml?var=dataSets:OFP2PhPl8FI||
