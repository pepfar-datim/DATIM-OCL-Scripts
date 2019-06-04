"""
Static class of constants for the DATIM-OCL project
"""


class DatimConstants(object):
    """ Static class of constants for the DATIM-OCL project """

    OCL_ETL_DEFINITIONS = {
        'MER-MSP': {
            'id': 'MER-MSP',
            'name': 'DATIM-DHIS2 MER-MSP Indicators',
            'import': {
                'ocl_dataset_endpoint': '/orgs/PEPFAR/collections/?verbose=true&limit=200',
                'fetch_dataset_ids_from_ocl': False,
                'dataset_ids': [],
                'repo_active_attribute': 'datim_sync_mer_msp',
                'query': '/api/dataElements.json?fields=id,code,name,shortName,lastUpdated,description,'
                         'categoryCombo[id,code,name,lastUpdated,created,'
                         'categoryOptionCombos[id,code,name,lastUpdated,created]],'
                         'dataSetElements[*,dataSet[id,name,shortName]]&'
                         'paging=false&filter=dataSetElements.dataSet.id:in:[{{active_dataset_ids}}]',
            },
            'export': {
                'openhim_endpoint': 'datim_mer_msp',
                'dhis2_presentation_url': '',
                'presentation_sort_column': 4,
                'conversion_method': 'dhis2diff_mer_msp',
                'show_build_row_method': 'build_mer_msp_indicator_output',
                'show_headers_key': 'mer_msp',
                'endpoint': '/orgs/PEPFAR/sources/MER-MSP/',
            }
        }
    }

    # Import batch IDs
    IMPORT_BATCH_MOH = 'MOH'  # don't use this one! switching to independent configuration each year
    IMPORT_BATCH_MOH_FY17 = 'MOH-FY17'
    IMPORT_BATCH_MOH_FY18 = 'MOH-FY18'
    IMPORT_BATCH_MOH_FY19 = 'MOH-FY19'
    IMPORT_BATCH_MER = 'MER'
    IMPORT_BATCH_MER_MSP = 'MER-MSP'
    IMPORT_BATCH_SIMS = 'SIMS'
    IMPORT_BATCH_MECHANISMS = 'Mechanisms'
    IMPORT_BATCH_TIERED_SUPPORT = 'Tiered-Support'  # Tiered Support is imported with init scripts, not a sync script

    # List of content categories
    SYNC_RESOURCE_TYPES = [
        IMPORT_BATCH_MOH,
        IMPORT_BATCH_MOH_FY17,
        IMPORT_BATCH_MOH_FY18,
        IMPORT_BATCH_MOH_FY19,
        IMPORT_BATCH_MER,
        IMPORT_BATCH_MER_MSP,
        IMPORT_BATCH_SIMS,
        IMPORT_BATCH_MECHANISMS,
        IMPORT_BATCH_TIERED_SUPPORT
    ]

    # OpenHIM Endpoints
    OPENHIM_ENDPOINT_MOH = 'datim-moh'  # Don't use this! Switching to independent configurations for each year
    OPENHIM_ENDPOINT_MOH_FY17 = 'datim-moh'
    OPENHIM_ENDPOINT_MOH_FY18 = 'datim-moh'
    OPENHIM_ENDPOINT_MOH_FY19 = 'datim-moh'
    OPENHIM_ENDPOINT_MER = 'datim-mer'
    OPENHIM_ENDPOINT_MER_MSP = 'datim-mer'
    OPENHIM_ENDPOINT_SIMS = 'datim-sims'
    OPENHIM_ENDPOINT_MECHANISMS = 'datim-mechanisms'
    OPENHIM_ENDPOINT_TIERED_SUPPORT = 'datim-tiered-support'

    # OCL Dataset Endpoints
    OCL_DATASET_ENDPOINT_MER = '/orgs/PEPFAR/collections/?verbose=true&limit=200'
    OCL_DATASET_ENDPOINT_MER_MSP = '/orgs/PEPFAR/collections/?verbose=true&limit=200'
    #OCL_DATASET_ENDPOINT_MECHANISMS = ''
    OCL_DATASET_ENDPOINT_SIMS = '/orgs/PEPFAR/collections/?q=SIMS&verbose=true&limit=200'
    OCL_DATASET_ENDPOINT_MOH = '/orgs/PEPFAR/collections/?verbose=true&limit=200'

    # OCL repository active attribute (must be set to True in OCL)
    REPO_ACTIVE_ATTR_MER = 'datim_sync_mer'
    REPO_ACTIVE_ATTR_MER_MSP = 'datim_sync_mer_msp'
    REPO_ACTIVE_ATTR_MECHANISMS = 'datim_sync_mechanisms'
    REPO_ACTIVE_ATTR_SIMS = 'datim_sync_sims'
    REPO_ACTIVE_ATTR_MOH_FY17 = 'datim_sync_moh_fy17'
    REPO_ACTIVE_ATTR_MOH_FY18 = 'datim_sync_moh_fy18'
    REPO_ACTIVE_ATTR_MOH_FY19 = 'datim_sync_moh_fy19'

    # DHIS2 Presentation URLs
    DHIS2_PRESENTATION_URL_MOH_FY18 = 'https://test.geoalign.datim.org/api/sqlViews/jxuvedhz3S3/data.{{format}}?var=dataSets:sfk9cyQSUyi'
    DHIS2_PRESENTATION_URL_MOH_FY19 = 'https://vshioshvili.datim.org/api/sqlViews/ioG5uxOYnZe/data.html+css?var=dataSets:OBhi1PUW3OL'
    DHIS2_PRESENTATION_URL_MOH = 'https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.{{format}}?var=dataSets:{{dataset}}'
    DHIS2_PRESENTATION_URL_MER = 'https://www.datim.org/api/sqlViews/DotdxKrNZxG/data.{{format}}?var=dataSets:{{dataset}}'
    DHIS2_PRESENTATION_URL_MER_MSP = ''
    DHIS2_PRESENTATION_URL_DEFAULT = 'https://dev-de.datim.org/api/sqlViews/{{sqlview}}/data.{{format}}'

    # SORT Column for exports
    MOH_PRESENTATION_SORT_COLUMN = 4
    MOH_FY18_PRESENTATION_SORT_COLUMN = 4
    MOH_FY19_PRESENTATION_SORT_COLUMN = 4
    MER_PRESENTATION_SORT_COLUMN = 4
    MER_MSP_PRESENTATION_SORT_COLUMN = 4
    SIMS_PRESENTATION_SORT_COLUMN = 2

    # Classifications for disags (categoryOptionCombos) for MOH Alignment
    DISAG_CLASSIFICATION_COARSE = 'coarse'
    DISAG_CLASSIFICATION_FINE = 'fine'
    DISAG_CLASSIFICATION_SEMI_FINE = 'semi-fine'
    DISAG_CLASSIFICATION_FINE_AND_SEMI_FINE = 'fine, semi-fine'
    DISAG_CLASSIFICATION_NA = 'n/a'
    DISAG_CLASSIFICATION_INVALID = 'INVALID'
    DISAG_CLASSIFICATIONS = [
        DISAG_CLASSIFICATION_COARSE,
        DISAG_CLASSIFICATION_FINE,
        DISAG_CLASSIFICATION_SEMI_FINE,
        DISAG_CLASSIFICATION_FINE_AND_SEMI_FINE,
        DISAG_CLASSIFICATION_NA,
        DISAG_CLASSIFICATION_INVALID
    ]

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

    # MER-MSP DHIS2 Queries
    MER_MSP_DHIS2_QUERIES = {
        'MER-MSP': {
            'id': 'MER-MSP',
            'name': 'DATIM-DHIS2 MER-MSP Indicators',
            'fy19': ['zUoy5hk8r0q', 'PyD4x9oFwxJ', 'KWRj80vEfHU', 'fi9yMqWLWVy', 'IZ71Y2mEBJF', 'ndp6aR3e1X3',
                     'pnlFw2gDGHD', 'gc4KOv8kGlI', 'FsYxodZiXyH', 'iJ4d5HdGiqG', 'GiqB9vjbdwb', 'EbZrNIkuPtc',
                     'nIHNMxuPUOR', 'C2G7IyPPrvD', 'sBv1dj90IX6', 'HiJieecLXxN', 'dNGGlQyiq9b', 'tTK9BhvS5t3',
                     'PH3bllbLw8W', 'N4X89PgW01w'],
            'fy18': ['WbszaIdCi92', 'uN01TT331OP', 'tz1bQ3ZwUKJ', 'BxIx51zpAjh', 'IZ71Y2mEBJF', 'mByGopCrDvL',
                     'XZfmcxHV4ie', 'jcS5GPoHDE0', 'USbxEt75liS', 'a4FRJ2P4cLf', 'l796jk9SW7q', 'BWBS39fydnX',
                     'eyI0UOWJnDk', 'X8sn5HE5inC', 'TdLjizPNezI', 'I8v9shsCZDS', 'lXQGzSqmreb', 'Ncq22MRC6gd'],
            'active_dataset_ids': [
                    'zUoy5hk8r0q', 'PyD4x9oFwxJ', 'KWRj80vEfHU', 'fi9yMqWLWVy', 'IZ71Y2mEBJF', 'ndp6aR3e1X3',
                    'pnlFw2gDGHD', 'gc4KOv8kGlI', 'FsYxodZiXyH', 'iJ4d5HdGiqG', 'GiqB9vjbdwb', 'EbZrNIkuPtc',
                    'nIHNMxuPUOR', 'C2G7IyPPrvD', 'sBv1dj90IX6', 'HiJieecLXxN', 'dNGGlQyiq9b', 'tTK9BhvS5t3',
                    'PH3bllbLw8W', 'N4X89PgW01w', 'WbszaIdCi92', 'uN01TT331OP', 'tz1bQ3ZwUKJ', 'BxIx51zpAjh',
                    'IZ71Y2mEBJF', 'mByGopCrDvL', 'XZfmcxHV4ie', 'jcS5GPoHDE0', 'USbxEt75liS', 'a4FRJ2P4cLf',
                    'l796jk9SW7q', 'BWBS39fydnX', 'eyI0UOWJnDk', 'X8sn5HE5inC', 'TdLjizPNezI', 'I8v9shsCZDS',
                    'lXQGzSqmreb', 'Ncq22MRC6gd'],
            'query': '/api/dataElements.json?fields=id,code,name,shortName,lastUpdated,description,'
                     'categoryCombo[id,code,name,lastUpdated,created,'
                     'categoryOptionCombos[id,code,name,lastUpdated,created]],'
                     'dataSetElements[*,dataSet[id,name,shortName]]&'
                     'paging=false&filter=dataSetElements.dataSet.id:in:[zUoy5hk8r0q,PyD4x9oFwxJ,KWRj80vEfHU,fi9yMqWLWVy,IZ71Y2mEBJF,ndp6aR3e1X3,pnlFw2gDGHD,gc4KOv8kGlI,FsYxodZiXyH,iJ4d5HdGiqG,GiqB9vjbdwb,EbZrNIkuPtc,nIHNMxuPUOR,C2G7IyPPrvD,sBv1dj90IX6,HiJieecLXxN,dNGGlQyiq9b,tTK9BhvS5t3,PH3bllbLw8W,N4X89PgW01w,WbszaIdCi92,uN01TT331OP,tz1bQ3ZwUKJ,BxIx51zpAjh,IZ71Y2mEBJF,mByGopCrDvL,XZfmcxHV4ie,jcS5GPoHDE0,USbxEt75liS,a4FRJ2P4cLf,l796jk9SW7q,BWBS39fydnX,eyI0UOWJnDk,X8sn5HE5inC,TdLjizPNezI,I8v9shsCZDS,lXQGzSqmreb,Ncq22MRC6gd]',
            'conversion_method': 'dhis2diff_mer_msp'
        }
    }

    # MOH DHIS2 Queries
    MOH_DHIS2_QUERIES = {
        'MOH': {
            'id': 'MOH',
            'name': 'DATIM MOH FY17 Indicators',
            'query': '/api/dataElements.json?fields=id,code,name,shortName,lastUpdated,description,'
                     'categoryCombo[id,code,name,lastUpdated,created,'
                     'categoryOptionCombos[id,code,name,lastUpdated,created]],'
                     'dataSetElements[*,dataSet[id,name,shortName]]&'
                     'paging=false&filter=dataSetElements.dataSet.id:in:[{{active_dataset_ids}}]',
            'conversion_method': 'dhis2diff_moh'
        }
    }

    # MOH-FY18 DHIS2 Queries
    MOH_FY18_DHIS2_QUERIES = {
        'MOH-FY18': {
            'id': 'MOH-FY18',
            'name': 'DATIM MOH FY18 Indicators',
            'query': '/api/dataElements.json?fields=id,code,name,shortName,lastUpdated,description,'
                     'categoryCombo[id,code,name,lastUpdated,created,'
                     'categoryOptionCombos[id,code,name,lastUpdated,created]],'
                     'dataSetElements[*,dataSet[id,name,shortName]]&'
                     'paging=false&filter=dataSetElements.dataSet.id:in:[sfk9cyQSUyi]',
            'conversion_method': 'dhis2diff_moh'
        }
    }

    # MOH-FY19 DHIS2 Queries
    # TODO: Verify that MOH-FY19 DHIS2 Queries works
    # NOTE: https://vshioshvili.datim.org/api/sqlViews/ioG5uxOYnZe/data.html+css?var=dataSets:OBhi1PUW3OL
    # https://vshioshvili.datim.org/api/dataElements.json?fields=id,code,name,shortName,lastUpdated,description,categoryCombo[id,code,name,lastUpdated,created,categoryOptionCombos[id,code,name,lastUpdated,created]],dataSetElements[*,dataSet[id,name,shortName]]&paging=false&filter=dataSetElements.dataSet.id:in:[OBhi1PUW3OL]
    MOH_FY19_DHIS2_QUERIES = {
        'MOH-FY19': {
            'id': 'MOH-FY19',
            'name': 'DATIM MOH FY19 Indicators',
            'query': '/api/dataElements.json?fields=id,code,name,shortName,lastUpdated,description,'
                     'categoryCombo[id,code,name,lastUpdated,created,'
                     'categoryOptionCombos[id,code,name,lastUpdated,created]],'
                     'dataSetElements[*,dataSet[id,name,shortName]]&'
                     'paging=false&filter=dataSetElements.dataSet.id:in:[OBhi1PUW3OL]',
            'conversion_method': 'dhis2diff_moh'
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

    # MOH OCL Export Definitions
    # TODO: Verify that the 'MOH' definition is not used and remove
    # TODO: Is DatimConstants.MOH_OCL_EXPORT_DEFS used at all?
    MOH_OCL_EXPORT_DEFS = {
        'MOH': {
            'import_batch': IMPORT_BATCH_MOH,
            'show_build_row_method': 'build_moh_indicator_output',
            'show_headers_key': 'moh',
            'endpoint': '/orgs/PEPFAR/sources/DATIM-MOH/'},
        'FY18': {
            'title': 'MER Results: MOH Facility Based FY18',
            'import_batch': IMPORT_BATCH_MOH_FY18,
            'show_build_row_method': 'build_moh_indicator_output',
            'show_headers_key': 'moh',
            'endpoint': '/orgs/PEPFAR/sources/DATIM-MOH-FY18/'},
        'FY19': {
            'title': 'MER Results: MOH Facility Based FY19',
            'import_batch': IMPORT_BATCH_MOH_FY19,
            'show_build_row_method': 'build_moh_indicator_output',
            'show_headers_key': 'moh',
            'endpoint': '/orgs/PEPFAR/sources/DATIM-MOH-FY19/'},
    }
    #JP: Removed 2019-05-23 until this is imported
    # 'FY17': {
    #   'title': 'MER Results: MOH Facility Based FY17',
    #   'import_batch': IMPORT_BATCH_MOH_FY17,
    #    'show_build_row_method': 'build_moh_indicator_output_fy17',
    #    'show_headers_key': 'moh-fy17',
    #    'endpoint': '/orgs/PEPFAR/sources/DATIM-MOH-FY17/'},

    # MOH FY18 OCL Export Definitions
    # TODO: Verify that this is not used and remove
    MOH_FY18_OCL_EXPORT_DEFS = {
        'MOH-FY18': {
            'import_batch': IMPORT_BATCH_MOH_FY18,
            'show_build_row_method': 'build_moh_fy18_indicator_output',
            'show_headers_key': 'moh_fy18',
            'endpoint': '/orgs/PEPFAR/sources/DATIM-MOH-FY18/'},
        }

    # MOH FY19 OCL Export Definitions
    # TODO: Verify that this is not used and remove
    MOH_FY19_OCL_EXPORT_DEFS = {
        'MOH-FY19': {
            'import_batch': IMPORT_BATCH_MOH_FY19,
            'show_build_row_method': 'build_moh_fy19_indicator_output',
            'show_headers_key': 'moh_fy19',
            'endpoint': '/orgs/PEPFAR/sources/DATIM-MOH-FY19/'},
        }

    # MER-MSP OCL Export Definitions
    MER_MSP_OCL_EXPORT_DEFS = {
        'MER-MSP': {
            'import_batch': IMPORT_BATCH_MER_MSP,
            'show_build_row_method': 'build_mer_msp_indicator_output',
            'show_headers_key': 'mer_msp',
            'endpoint': '/orgs/PEPFAR/sources/MER-MSP/'},
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
