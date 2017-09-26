Feature: Indicator Metadata Served in html
  This test is designed to prove that the OCL indicator html Requests provide metadata that matches the indicator DATIM html Requests
  End-users may request to get html version of the DATIM Indicator Metadata and it successfully produces the same resut from OCL as one would get from DATIM.  

Scenario: 
	Given That the OCL system has been populated with Datim metadata
		And OCL system is being regreshed with updated metadta
	Given that the end-user has navigated to the OpenHIE metadata page
		And the end-user desires metadata to be delivered in html
		And the end-user has determined which type of metadata that is desired
	When the end-user clicks on OCL html metadata link for <OCLhtmlResult>
		Then the <OCLhtmlResult> matches  the result when the end user clicks on <PEPFARhtmlResult>
 	Examples:
    |OCLhtmlResult|PEPFARhtmlResult|
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.html?var=dataSets:kkXf2zXqTM0||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.html?var=dataSets:MqNLEXmzIzr||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.html?var=dataSets:K7FMzevlBAp||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.html?var=dataSets:UZ2PLqSe5Ri||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.html?var=dataSets:CGoi5wjLHDy||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.html?var=dataSets:LWE9GdlygD5||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.html?var=dataSets:tG2hjDIaYQD||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.html?var=dataSets:Kxfk0KVsxDn||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.html?var=dataSets:hgOW2BSUDaN||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.html?var=dataSets:Awq346fnVLV||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.html?var=dataSets:Awq346fnVLV||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.html?var=dataSets:ovmC3HNi4LN||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.html?var=dataSets:CGoi5wjLHDy||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.html?var=dataSets:zTgQ3MvHYtk||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.html?var=dataSets:KwkuZhKulqs||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.html?var=dataSets:eAlxMKMZ9GV||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.html?var=dataSets:PkmLpkrPdQG||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.html?var=dataSets:zeUCqFKIBDD||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.html?var=dataSets:AitXBHsC7RA||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.html?var=dataSets:BuRoS9i851o||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.html?var=dataSets:jEzgpBt5Icf||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.html?var=dataSets:ePndtmDbOJj||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.html?var=dataSets:AvmGbcurn4K||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.html?var=dataSets:O8hSwgCbepv||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.html?var=dataSets:bqiB5G6qgzn||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.html?var=dataSets:YWZrOj5KS1c||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.html?var=dataSets:c7Gwzm5w9DE||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.html?var=dataSets:pTuDWXzkAkJ||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.html?var=dataSets:OFP2PhPl8FI||

   
