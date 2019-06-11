""" Static helper class for DatimSyncMoh objects """
import datimconstants

class DatimSyncMohHelper(object):
    """ Static helper class for DatimSyncMoh objects """

    CLASSIFIER_FY18_COARSE_UIDS = ['IXkZ7eWtFHs', 'iyANolnH3mk']
    CLASSIFIER_FY18_INVALID_1 = '1-9, Female'
    CLASSIFIER_FY18_INVALID_2 = '1-9, Male'
    CLASSIFIER_FY18_INVALID_3 = '<1, Female'
    CLASSIFIER_FY18_INVALID_4 = '<1, Male'
    CLASSIFIER_FY18_INVALID_5 = '30-49'
    CLASSIFIER_FY18_SEMI_FINE_1 = '25-49'
    CLASSIFIER_FY18_FINE_1 = '25-29'
    CLASSIFIER_FY18_FINE_2 = '30-34'
    CLASSIFIER_FY18_FINE_3 = '35-39'
    CLASSIFIER_FY18_FINE_4 = '40-49'

    @staticmethod
    def get_disag_classification(period='', de_code='', de_uid='', coc_name=''):
        if period == 'FY18':
            return DatimSyncMohHelper.get_disag_classification_fy18(de_code=de_code, de_uid=de_uid, coc_name=coc_name)
        elif period == 'FY19':
            return DatimSyncMohHelper.get_disag_classification_fy18(de_code=de_code, de_uid=de_uid, coc_name=coc_name)
        return ''

    @staticmethod
    def get_disag_classification_fy18(de_code='', de_uid='', coc_name=''):
        """
        Python implementation of the classification logic embedded in the DHIS2 SqlView
        (refer to https://test.geoalign.datim.org/api/sqlViews/jxuvedhz3S3).
        Here's the SQL version:
            case
            when de.code like '%_Age_Agg%' or de.uid = 'IXkZ7eWtFHs' or de.uid = 'iyANolnH3mk' then 'coarse'
            when coc.name = 'default' then 'n/a'
            when coc.name like '1-9, Female%' or coc.name like '1-9, Male%' or coc.name like '<1, Female%' or coc.name
                like '<1, Male%' or coc.name like '30-49%' then 'INVALID'
            when coc.name like '25-49%' then 'semi-fine'
            when coc.name like '25-29%' or coc.name like '30-34%' or coc.name like '35-39%' or coc.name like '40-49%'
                then 'fine'
            else 'fine, semi-fine'
            end as classification
        :param de_code: DataElement code
        :param de_uid: DataElement UID
        :param coc_name: CategoryOptionCombo name
        :return: <string> A member of DatimConstants.DISAG_CLASSIFICATIONS
        """
        if '_Age_Agg' in de_code or de_uid in DatimSyncMohHelper.CLASSIFIER_FY18_COARSE_UIDS:
            return datimconstants.DatimConstants.DISAG_CLASSIFICATION_COARSE
        elif coc_name == 'default':
            return datimconstants.DatimConstants.DISAG_CLASSIFICATION_NA
        elif (coc_name[:len(DatimSyncMohHelper.CLASSIFIER_FY18_INVALID_1)] == DatimSyncMohHelper.CLASSIFIER_FY18_INVALID_1 or
              coc_name[:len(DatimSyncMohHelper.CLASSIFIER_FY18_INVALID_2)] == DatimSyncMohHelper.CLASSIFIER_FY18_INVALID_2 or
              coc_name[:len(DatimSyncMohHelper.CLASSIFIER_FY18_INVALID_3)] == DatimSyncMohHelper.CLASSIFIER_FY18_INVALID_3 or
              coc_name[:len(DatimSyncMohHelper.CLASSIFIER_FY18_INVALID_4)] == DatimSyncMohHelper.CLASSIFIER_FY18_INVALID_4 or
              coc_name[:len(DatimSyncMohHelper.CLASSIFIER_FY18_INVALID_5)] == DatimSyncMohHelper.CLASSIFIER_FY18_INVALID_5):
            return datimconstants.DatimConstants.DISAG_CLASSIFICATION_INVALID
        elif coc_name[:len(DatimSyncMohHelper.CLASSIFIER_FY18_SEMI_FINE_1)] == DatimSyncMohHelper.CLASSIFIER_FY18_SEMI_FINE_1:
            return datimconstants.DatimConstants.DISAG_CLASSIFICATION_SEMI_FINE
        elif (coc_name[:len(DatimSyncMohHelper.CLASSIFIER_FY18_FINE_1)] == DatimSyncMohHelper.CLASSIFIER_FY18_FINE_1 or
              coc_name[:len(DatimSyncMohHelper.CLASSIFIER_FY18_FINE_2)] == DatimSyncMohHelper.CLASSIFIER_FY18_FINE_2 or
              coc_name[:len(DatimSyncMohHelper.CLASSIFIER_FY18_FINE_3)] == DatimSyncMohHelper.CLASSIFIER_FY18_FINE_3 or
              coc_name[:len(DatimSyncMohHelper.CLASSIFIER_FY18_FINE_4)] == DatimSyncMohHelper.CLASSIFIER_FY18_FINE_4):
            return datimconstants.DatimConstants.DISAG_CLASSIFICATION_FINE
        return datimconstants.DatimConstants.DISAG_CLASSIFICATION_FINE_AND_SEMI_FINE

    @staticmethod
    def get_disag_classification_fy19(de_code='', de_uid='', coc_name=''):
        """
        Python implementation of the classification logic embedded in the DHIS2 SqlView
        (refer to https://vshioshvili.datim.org/api/sqlViews/ioG5uxOYnZe).
        Here's the SQL version:
            case
            when de.code like '%_Age_Agg%' then 'coarse'
            when de.code like '%_Age_%' then 'fine'
            else 'n/a'
            end as classification
        :param de_code: DataElement code
        :param de_uid: DataElement UID
        :param coc_name: CategoryOptionCombo name
        :return: <string> A member of DatimConstants.DISAG_CLASSIFICATIONS
        """
        if '_Age_Agg' in de_code:
            return datimconstants.DatimConstants.DISAG_CLASSIFICATION_COARSE
        elif '_Age_' in de_code:
            return datimconstants.DatimConstants.DISAG_CLASSIFICATION_FINE
        return datimconstants.DatimConstants.DISAG_CLASSIFICATION_NA
