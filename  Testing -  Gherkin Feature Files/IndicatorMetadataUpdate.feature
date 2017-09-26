Feature: Indicator Metadata Update
  This test is designed to prove that indicator updates in DHIS2 are propigated to OCL and that data matches 

Scenario: Indicator metadata is updated in DHIS2 and needs to be synced with OCL
    Given That a user logs into DHIS2
        And adds a DHIS2 indicator data element 
        And updates another indicator data element's name
        And updates another indicator data element's short name
    When Indicator metadata sync is triggered
        Then the metadata sync successfully completes 
        And the new indicator is present in OCL 
        And the indicator updates are present in OCL
        And the OCL system and the DHIS2 system have the same indicator data when compared

Scenario: Indicator metadata has not been updated, but the sync process runs
    Given That OCL and DHIS2 are already synchronized
    When Indicator metadata sync is triggered
        Then the metadata sync successfully completes 
        And the OCL system and the DHIS2 system have the same indicator data when compared