Feature: Indicator Metadata Served in JSON
  This test is designed to prove that the OCL indicator JSON Requests provide metadata that matches the indicator DATIM JSON Requests
  End-users may request to get JSON version of the DATIM Indicator Metadata and it successfully produces the same resut from OCL as one would get from DATIM.  

Scenario: 
	Given That the OCL system has been populated with Datim metadata
		And OCL system is being regreshed with updated metadta
	Given that the end-user has navigated to the OpenHIE metadata page
		And the end-user desires metadata to be delivered in JSON
		And the end-user has determined which type of metadata that is desired
	When the end-user clicks on OCL JSON metadata link for <OCLJSONResult>
		Then the <OCLJSONResult> matches  the result when the end user clicks on <PEPFAR JSON Result>
		Examples:
    |OCL JSON Result|PEPFAR JSON Result|
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.json?var=dataSets:kkXf2zXqTM0||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.json?var=dataSets:MqNLEXmzIzr||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.json?var=dataSets:K7FMzevlBAp||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.json?var=dataSets:UZ2PLqSe5Ri||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.json?var=dataSets:CGoi5wjLHDy||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.json?var=dataSets:LWE9GdlygD5||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.json?var=dataSets:tG2hjDIaYQD||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.json?var=dataSets:Kxfk0KVsxDn||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.json?var=dataSets:hgOW2BSUDaN||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.json?var=dataSets:Awq346fnVLV||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.json?var=dataSets:Awq346fnVLV||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.json?var=dataSets:ovmC3HNi4LN||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.json?var=dataSets:CGoi5wjLHDy||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.json?var=dataSets:zTgQ3MvHYtk||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.json?var=dataSets:KwkuZhKulqs||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.json?var=dataSets:eAlxMKMZ9GV||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.json?var=dataSets:PkmLpkrPdQG||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.json?var=dataSets:zeUCqFKIBDD||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.json?var=dataSets:AitXBHsC7RA||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.json?var=dataSets:BuRoS9i851o||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.json?var=dataSets:jEzgpBt5Icf||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.json?var=dataSets:ePndtmDbOJj||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.json?var=dataSets:AvmGbcurn4K||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.json?var=dataSets:O8hSwgCbepv||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.json?var=dataSets:bqiB5G6qgzn||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.json?var=dataSets:YWZrOj5KS1c||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.json?var=dataSets:c7Gwzm5w9DE||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.json?var=dataSets:pTuDWXzkAkJ||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.json?var=dataSets:OFP2PhPl8FI||

    
   