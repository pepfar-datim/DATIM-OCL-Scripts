class DatimConstants:

    # Import batch IDs
    IMPORT_BATCH_MER = 'MER'
    IMPORT_BATCH_SIMS = 'SIMS'
    IMPORT_BATCH_MECHANISMS = 'Mechanisms'
    IMPORT_BATCH_TIERED_SUPPORT = 'Tiered-Support'  # Tiered Support is imported with init scripts, not a sync script

    # List of content categories
    SYNC_RESOURCE_TYPES = [
        IMPORT_BATCH_MER,
        IMPORT_BATCH_SIMS,
        IMPORT_BATCH_MECHANISMS,
        IMPORT_BATCH_TIERED_SUPPORT
    ]

    # OpenHIM Endpoints
    OPENHIM_ENDPOINT_MER = 'datim-mer'
    OPENHIM_ENDPOINT_SIMS = 'datim-sims'
    OPENHIM_ENDPOINT_MECHANISMS = 'datim-mechanisms'
    OPENHIM_ENDPOINT_TIERED_SUPPORT = 'datim-tiered-support'

    # DHIS2 Presentation URLs
    DHIS2_PRESENTATION_URL_MER = 'https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.{{format}}?var=dataSets:{{dataset}}'
    DHIS2_PRESENTATION_URL_DEFAULT = 'https://dev-de.datim.org/api/sqlViews/{{sqlview}}/data.{{format}}'

    # E2E Testing
    MER_PRESENTATION_SORT_COLUMN = 4
    SIMS_PRESENTATION_SORT_COLUMN = 2

    # SIMS DHIS2 Queries
    SIMS_DHIS2_QUERIES = {
        'SimsAssessmentTypes': {
            'id': 'SimsAssessmentTypes',
            'name': 'DATIM-DHIS2 SIMS Assessment Types',
            'query': 'api/dataElements.json?fields=name,code,id,valueType,lastUpdated,dataElementGroups[id,name]&'
                     'order=code:asc&paging=false&filter=dataElementGroups.id:in:[{{active_dataset_ids}}]',
            'conversion_method': 'dhis2diff_sims_assessment_types'
        },
        'SimsOptionSets': {
            'id': 'SimsOptionSets',
            'name': 'DATIM-DHIS2 SIMS Option Sets',
            'query': 'api/optionSets/?fields=id,name,lastUpdated,options[id,code,name]&'
                     'filter=name:like:SIMS%20v2&paging=false&order=name:asc',
            'conversion_method': 'dhis2diff_sims_option_sets'
        }
    }

    # MER DHIS2 Queries
    MER_DHIS2_QUERIES = {
        'MER': {
            'id': 'MER',
            'name': 'DATIM-DHIS2 MER Indicators',
            'query': '/api/dataElements.json?fields=id,code,name,shortName,lastUpdated,description,'
                     'categoryCombo[id,code,name,lastUpdated,created,'
                     'categoryOptionCombos[id,code,name,lastUpdated,created]],'
                     'dataSetElements[*,dataSet[id,name,shortName]]&'
                     'paging=false&filter=dataSetElements.dataSet.id:in:[{{active_dataset_ids}}]',
            'conversion_method': 'dhis2diff_mer'
        }
    }

    # Mechanisms DHIS2 Queries
    MECHANISMS_DHIS2_QUERIES = {
        'Mechanisms': {
            'id': 'Mechanisms',
            'name': 'DATIM-DHIS2 Funding Mechanisms',
            'query': 'api/categoryOptionCombos.json?fields=id,code,name,created,lastUpdated,'
                     'categoryOptions[id,endDate,startDate,organisationUnits[code,name],'
                     'categoryOptionGroups[id,name,code,groupSets[id,name]]]&order=code:asc&filter=categoryCombo.id:eq:wUpfppgjEza&paging=false',
            'conversion_method': 'dhis2diff_mechanisms'
        }
    }

    # MER OCL Export Definitions
    MER_OCL_EXPORT_DEFS = {
        'MER': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/sources/MER/'},
        'MER-R-Facility-DoD-FY17Q1': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/MER-R-Facility-DoD-FY17Q1/'},
        'MER-R-Facility-DoD-FY17Q2': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/MER-R-Facility-DoD-FY17Q2/'},
        'MER-R-Facility-DoD-FY16Q4': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/MER-R-Facility-DoD-FY16Q4/'},
        'HC-R-COP-Prioritization-SNU-USG-FY16Q4': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/HC-R-COP-Prioritization-SNU-USG-FY16Q4/'},
        'HC-R-Narratives-USG-FY16Q1Q2Q3': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/HC-R-Narratives-USG-FY16Q1Q2Q3/'},
        'HC-R-Narratives-USG-FY16Q4': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/HC-R-Narratives-USG-FY16Q4/'},
        'HC-R-Narratives-USG-FY17Q1': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/HC-R-Narratives-USG-FY17Q1/'},
        'HC-R-Narratives-USG-FY17Q2': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/HC-R-Narratives-USG-FY17Q2/'},
        'HC-R-Operating-Unit-Level-USG-FY16Q1Q2Q3': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/HC-R-Operating-Unit-Level-USG-FY16Q1Q2Q3/'},
        'HC-R-Operating-Unit-Level-USG-FY16Q4': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/HC-R-Operating-Unit-Level-USG-FY16Q4/'},
        'HC-T-COP-Prioritization-SNU-USG-FY17': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/HC-T-COP-Prioritization-SNU-USG-FY17/'},
        'HC-T-COP-Prioritization-SNU-USG-FY18': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/HC-T-COP-Prioritization-SNU-USG-FY18/'},
        'HC-T-Narratives-USG-FY16': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/HC-T-Narratives-USG-FY16/'},
        'HC-T-Narratives-USG-FY17': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/HC-T-Narratives-USG-FY17/'},
        'HC-T-Operating-Unit-Level-USG-FY16': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/HC-T-Operating-Unit-Level-USG-FY16/'},
        'HC-T-Operating-Unit-Level-USG-FY17': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/HC-T-Operating-Unit-Level-USG-FY17/'},
        'HC-T-Operating-Unit-Level-USG-FY18': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/HC-T-Operating-Unit-Level-USG-FY18/'},
        'MER-R-Community-DoD-FY16Q1Q2Q3': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/MER-R-Community-DoD-FY16Q1Q2Q3/'},
        'MER-R-Community-DoD-FY16Q4': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/MER-R-Community-DoD-FY16Q4/'},
        'MER-R-Community-DoD-FY17Q1': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/MER-R-Community-DoD-FY17Q1/'},
        'MER-R-Community-DoD-FY17Q2': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/MER-R-Community-DoD-FY17Q2/'},
        'MER-R-Community-FY16Q1Q2Q3': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/MER-R-Community-FY16Q1Q2Q3/'},
        'MER-R-Community-FY16Q4': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/MER-R-Community-FY16Q4/'},
        'MER-R-Community-FY17Q1': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/MER-R-Community-FY17Q1/'},
        'MER-R-Community-FY17Q2': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/MER-R-Community-FY17Q2/'},
        'MER-R-Facility-FY16Q4': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/MER-R-Facility-FY16Q4/'},
        'MER-R-Facility-FY17Q1': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/MER-R-Facility-FY17Q1/'},
        'MER-R-Facility-FY17Q2': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/MER-R-Facility-FY17Q2/'},
        'MER-R-Medical-Store-FY16Q1Q2Q3': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/MER-R-Medical-Store-FY16Q1Q2Q3/'},
        'MER-R-Medical-Store-FY16Q4': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/MER-R-Medical-Store-FY16Q4/'},
        'MER-R-Medical-Store-FY17Q1': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/MER-R-Medical-Store-FY17Q1/'},
        'MER-R-Narratives-IM-FY16Q1Q2Q3': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/MER-R-Narratives-IM-FY16Q1Q2Q3/'},
        'MER-R-Narratives-IM-FY16Q4': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/MER-R-Narratives-IM-FY16Q4/'},
        'MER-R-Narratives-IM-FY17Q1': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/MER-R-Narratives-IM-FY17Q1/'},
        'MER-R-Narratives-IM-FY17Q2': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/MER-R-Narratives-IM-FY17Q2/'},
        'MER-R-Operating-Unit-Level-IM-FY16Q1Q2Q3': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/MER-R-Operating-Unit-Level-IM-FY16Q1Q2Q3/'},
        'MER-R-Operating-Unit-Level-IM-FY16Q4': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/MER-R-Operating-Unit-Level-IM-FY16Q4/'},
        'MER-R-Operating-Unit-Level-IM-FY17Q1': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/MER-R-Operating-Unit-Level-IM-FY17Q1/'},
        'MER-R-Operating-Unit-Level-IM-FY17Q2': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/MER-R-Operating-Unit-Level-IM-FY17Q2/'},
        'MER-T-Community-DoD-FY16': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/MER-T-Community-DoD-FY16/'},
        'MER-T-Community-DoD-FY17': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/MER-T-Community-DoD-FY17/'},
        'MER-T-Community-DoD-FY18': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/MER-T-Community-DoD-FY18/'},
        'MER-T-Community-FY17': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/MER-T-Community-FY17/'},
        'MER-T-Community-FY18': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/MER-T-Community-FY18/'},
        'MER-T-Facility-DoD-FY16': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/MER-T-Facility-DoD-FY16/'},
        'MER-T-Facility-DoD-FY17': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/MER-T-Facility-DoD-FY17/'},
        'MER-T-Facility-DoD-FY18': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/MER-T-Facility-DoD-FY18/'},
        'MER-T-Facility-FY16': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/MER-T-Facility-FY16/'},
        'MER-T-Facility-FY17': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/MER-T-Facility-FY17/'},
        'MER-T-Facility-FY18': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/MER-T-Facility-FY18/'},
        'MER-T-Narratives-IM-FY16': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/MER-T-Narratives-IM-FY16/'},
        'MER-T-Narratives-IM-FY17': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/MER-T-Narratives-IM-FY17/'},
        'MER-T-Narratives-IM-FY18': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/MER-T-Narratives-IM-FY18/'},
        'MER-T-Operating-Unit-Level-IM-FY16': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/MER-T-Operating-Unit-Level-IM-FY16/'},
        'MER-T-Operating-Unit-Level-IM-FY17': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/MER-T-Operating-Unit-Level-IM-FY17/'},
        'MER-T-Operating-Unit-Level-IM-FY18': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/MER-T-Operating-Unit-Level-IM-FY18/'},
        'Planning-Attributes-COP-Prioritization-National-FY18': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/Planning-Attributes-COP-Prioritization-National-FY18/'},
        'Planning-Attributes-COP-Prioritization-SNU-FY18': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/Planning-Attributes-COP-Prioritization-SNU-FY18/'},
        'MER-R-Facility-DoD-FY16Q1Q2Q3': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/MER-R-Facility-DoD-FY16Q1Q2Q3/'},
        'MER-T-Community-FY16': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/MER-T-Community-FY16/'},
        'MER-R-Facility-FY17Q4': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/MER-R-Facility-FY17Q4/'},
        'MER-R-Community-FY17Q4': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/MER-R-Community-FY17Q4/'},
        'MER-R-Facility-DoD-FY17Q4': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/MER-R-Facility-DoD-FY17Q4/'},
        'MER-R-Community-DoD-FY17Q4': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/MER-R-Community-DoD-FY17Q4/'},
        'MER-R-Medical-Store-FY17Q4': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/MER-R-Medical-Store-FY17Q4/'},
        'MER-R-Narratives-IM-FY17Q4': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/MER-R-Narratives-IM-FY17Q4/'},
        'MER-R-Operating-Unit-Level-IM-FY17Q4': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/MER-R-Operating-Unit-Level-IM-FY17Q4/'},
        'HC-R-Narratives-USG-FY17Q4': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/HC-R-Narratives-USG-FY17Q4/'},
        'HC-R-Operating-Unit-Level-USG-FY17Q4': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/HC-R-Operating-Unit-Level-USG-FY17Q4/'},
        'HC-R-COP-Prioritization-SNU-USG-FY17Q4': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/HC-R-COP-Prioritization-SNU-USG-FY17Q4/'},
        'MER-R-Facility-FY16Q1Q2Q3': {
            'import_batch': IMPORT_BATCH_MER,
            'show_build_row_method': 'build_mer_indicator_output',
            'show_headers_key': 'mer',
            'endpoint': '/orgs/PEPFAR/collections/MER-R-Facility-FY16Q1Q2Q3/'},
    }
    # INACTIVE_MER_OCL_EXPORT_DEFS

    # SIMS OCL Export Definitions
    SIMS_OCL_EXPORT_DEFS = {
        'SIMS-Source': {
            'import_batch': IMPORT_BATCH_SIMS,
            'show_build_row_method': 'build_sims_row',
            'show_headers_key': 'sims',
            'endpoint': '/orgs/PEPFAR/sources/SIMS/'},
        'SIMS2-Above-Site': {
            'import_batch': IMPORT_BATCH_SIMS,
            'show_build_row_method': 'build_sims_row',
            'show_headers_key': 'sims',
            'endpoint': '/orgs/PEPFAR/collections/SIMS2-Above-Site/',
            'dhis2_sqlview_id': 'lrdLdQe630Q'},
        'SIMS2-Community': {
            'import_batch': IMPORT_BATCH_SIMS,
            'show_build_row_method': 'build_sims_row',
            'show_headers_key': 'sims',
            'endpoint': '/orgs/PEPFAR/collections/SIMS2-Community/',
            'dhis2_sqlview_id': 'jJLtJha39hn'},
        'SIMS2-Facility': {
            'import_batch': IMPORT_BATCH_SIMS,
            'show_build_row_method': 'build_sims_row',
            'show_headers_key': 'sims',
            'endpoint': '/orgs/PEPFAR/collections/SIMS2-Facility/',
            'dhis2_sqlview_id': 'd14tCDY7CBv'},
        'SIMS3-Above-Site': {
            'import_batch': IMPORT_BATCH_SIMS,
            'show_build_row_method': 'build_sims_row',
            'show_headers_key': 'sims',
            'endpoint': '/orgs/PEPFAR/collections/SIMS3-Above-Site/',
            'dhis2_sqlview_id': 'wL1TY929jCS'},
        'SIMS3-Community': {
            'import_batch': IMPORT_BATCH_SIMS,
            'show_build_row_method': 'build_sims_row',
            'show_headers_key': 'sims',
            'endpoint': '/orgs/PEPFAR/collections/SIMS3-Community/',
            'dhis2_sqlview_id': 'PB2eHiURtwS'},
        'SIMS3-Facility': {
            'import_batch': IMPORT_BATCH_SIMS,
            'show_build_row_method': 'build_sims_row',
            'show_headers_key': 'sims',
            'endpoint': '/orgs/PEPFAR/collections/SIMS3-Facility/',
            'dhis2_sqlview_id': 'uMvWjOo31wt'},
        'SIMS-Option-Sets': {
            'import_batch': IMPORT_BATCH_SIMS,
            'show_build_row_method': 'build_sims_option_sets_row',
            'show_headers_key': 'option_sets',
            'endpoint': '/orgs/PEPFAR/collections/SIMS-Option-Sets/',
            'dhis2_sqlview_id': 'JlRJO4gqiu7'},
    }

    # Mechanisms OCL Export Definitions
    MECHANISMS_OCL_EXPORT_DEFS = {
        'Mechanisms': {
            'import_batch': IMPORT_BATCH_MECHANISMS,
            'show_build_row_method': 'build_mechanism_row',
            'show_headers_key': 'mechanisms',
            'subtitle': 'View of mechanisms, partners, agencies, OUs and start and end dates for each mechanism',
            'endpoint': '/orgs/PEPFAR/sources/Mechanisms/',
            'dhis2_sqlview_id': 'fgUtV6e9YIX'},
    }

    # Tiered Support OCL Export Definitions
    TIERED_SUPPORT_OCL_EXPORT_DEFS = {
        'dataelements': {
            'import_batch': IMPORT_BATCH_TIERED_SUPPORT,
            'show_build_row_method': 'build_tiered_support_data_element_row',
            'show_headers_key': 'tiered_support_data_elements',
            'endpoint': '/orgs/PEPFAR/sources/Tiered-Site-Support/',
            'dhis2_sqlview_id': 'l8pThk1VnTC'},
        'options': {
            'import_batch': IMPORT_BATCH_TIERED_SUPPORT,
            'show_build_row_method': 'build_tiered_support_option_row',
            'show_headers_key': 'tiered_support_options',
            'endpoint': '/orgs/PEPFAR/sources/Tiered-Site-Support/',
            'dhis2_sqlview_id': 'ELFCPUHushX'},
    }
