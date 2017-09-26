Feature: Indicator Metadata Served in CSV
  This test is designed to prove that the OCL indicator CSV Requests provide metadata that matches the indicator DATIM CSV Requests
  End-users may request to get CSV version of the DATIM Indicator Metadata and it successfully produces the same resut from OCL as one would get from DATIM.  

Scenario: 
	Given That the OCL system has been populated with Datim metadata
		And OCL system is being regreshed with updated metadta
	Given that the end-user has navigated to the OpenHIE metadata page
		And the end-user desires metadata to be delivered in CSV
		And the end-user has determined which type of metadata that is desired
	When the end-user clicks on OCL CSV metadata link for <OCLCSVResult>
		Then the <OCLCSVResult> matches  the result when the end user clicks on <PEPFARCSVResult>
		Examples:
    |OCLCSVResult|PEPFARCSVResult|
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.csv?var=dataSets:kkXf2zXqTM0||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.csv?var=dataSets:MqNLEXmzIzr||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.csv?var=dataSets:K7FMzevlBAp||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.csv?var=dataSets:UZ2PLqSe5Ri||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.csv?var=dataSets:CGoi5wjLHDy||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.csv?var=dataSets:LWE9GdlygD5||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.csv?var=dataSets:tG2hjDIaYQD||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.csv?var=dataSets:Kxfk0KVsxDn||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.csv?var=dataSets:hgOW2BSUDaN||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.csv?var=dataSets:Awq346fnVLV||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.csv?var=dataSets:CS958XpDaUf||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.csv?var=dataSets:ovmC3HNi4LN||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.csv?var=dataSets:CGoi5wjLHDy||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.csv?var=dataSets:zTgQ3MvHYtk||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.csv?var=dataSets:KwkuZhKulqs||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.csv?var=dataSets:eAlxMKMZ9GV||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.csv?var=dataSets:PkmLpkrPdQG||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.csv?var=dataSets:zeUCqFKIBDD||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.csv?var=dataSets:AitXBHsC7RA||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.csv?var=dataSets:BuRoS9i851o||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.csv?var=dataSets:jEzgpBt5Icf||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.csv?var=dataSets:ePndtmDbOJj||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.csv?var=dataSets:AvmGbcurn4K||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.csv?var=dataSets:O8hSwgCbepv||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.csv?var=dataSets:bqiB5G6qgzn||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.csv?var=dataSets:YWZrOj5KS1c||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.csv?var=dataSets:c7Gwzm5w9DE||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.csv?var=dataSets:pTuDWXzkAkJ||
    |https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.csv?var=dataSets:OFP2PhPl8FI||


    