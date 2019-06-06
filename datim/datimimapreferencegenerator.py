import datimbase


class DatimImapReferenceGenerator(datimbase.DatimBase):
    """
    Class to generate collections and references for country-level mappings to DATIM-MOH
    indicators as part of the DATIM Metadata initiative
    """

    def __init__(self, oclenv='', oclapitoken='', imap_input=None):
        datimbase.DatimBase.__init__(self)
        self.imap_input = imap_input
        self.oclenv = oclenv
        self.oclapitoken = oclapitoken

        # Build MOH source URI (e.g. /orgs/DATIM-MOH-UG/sources/DATIM-Alignment-Indicators/)
        moh_owner_type_url_part = datimbase.DatimBase.owner_type_to_stem(self.DATIM_MOH_COUNTRY_OWNER_TYPE)
        if moh_owner_type_url_part:
            self.moh_source_uri = '/%s/%s/sources/%s/' % (
                moh_owner_type_url_part, self.imap_input.country_org, self.DATIM_MOH_COUNTRY_SOURCE_ID)
        else:
            msg = 'ERROR: Invalid owner_type "%s"' % self.DATIM_MOH_COUNTRY_OWNER_TYPE
            self.log(msg)
            raise Exception(msg)

        self.refs_by_collection = {}
        self.oclapiheaders = {
            'Authorization': 'Token ' + oclapitoken,
            'Content-Type': 'application/json'
        }

    def process_imap(self, country_source_export=None, num_rows=0):
        """ Return list of OCL-formatted JSON to import IMAP references """

        row_i = 0
        for csv_row in self.imap_input:
            # 
            if num_rows and row_i >= num_rows:
                break
            row_i += 1
            self.generate_reference(csv_row, country_source_export)

        # Post-processing -- turn into OCL-formatted JSON for import
        import_list = []
        for c in self.refs_by_collection:
            import_json = {
                    'type':'Reference',
                    'owner':self.imap_input.country_org,
                    'owner_type':self.DATIM_MOH_COUNTRY_OWNER_TYPE,
                    'collection':c,
                    'data':{'expressions':self.refs_by_collection[c]}
                }
            import_list.append(import_json)
        return import_list

    def generate_reference(self, csv_row, country_source_export):
        """ Generate collection references and other required resources for a single CSV row """

        if 'Country Collection ID' not in csv_row or not csv_row['Country Collection ID']:
            return

        # Add references to DATIM concepts/mappings if first use of this collection
        collection_id = csv_row['Country Collection ID']
        if collection_id not in self.refs_by_collection:
            # DATIM HAS OPTION Mapping
            mapping_id = self.get_mapping_uri_from_export(
                country_source_export,
                csv_row['DATIM From Concept URI'],
                datimbase.DatimBase.DATIM_MOH_MAP_TYPE_COUNTRY_OPTION,
                csv_row['DATIM To Concept URI'])
            self.refs_by_collection[collection_id] = [mapping_id]

            # Add DATIM From concept
            self.refs_by_collection[collection_id].append(csv_row['DATIM From Concept URI'])

            # Add DATIM To concept
            self.refs_by_collection[collection_id].append(csv_row['DATIM To Concept URI'])

        # Now add the country mapping reference
        mapping_id = self.get_mapping_uri_from_export(
            country_source_export,
            csv_row['Country From Concept URI'],
            csv_row['Country Map Type'],
            csv_row['Country To Concept URI'])
        self.refs_by_collection[collection_id].append(mapping_id)

        # Add country From-Concept
        # versioned_uri = DatimImapReferenceGenerator.get_versioned_concept_uri_from_export(
        #     country_source_export, csv_row['Country From Concept URI'])
        # self.refs_by_collection[collection_id].append(versioned_uri)
        self.refs_by_collection[collection_id].append(csv_row['Country From Concept URI'])

        # Add country To-Concept
        if csv_row['MOH_Disag_ID'] == datimbase.DatimBase.NULL_DISAG_ID:
            # Add null_disag from PEPFAR/DATIM-MOH source instead
            self.refs_by_collection[collection_id].append(
                datimbase.DatimBase.NULL_DISAG_ENDPOINT)
        else:
            # versioned_uri = DatimImapReferenceGenerator.get_versioned_concept_uri_from_export(
            #     country_source_export, csv_row['Country To Concept URI'])
            # self.refs_by_collection[collection_id].append(versioned_uri)
            self.refs_by_collection[collection_id].append(csv_row['Country To Concept URI'])

    @staticmethod
    def get_versioned_concept_uri_from_export(country_source_export, nonversioned_uri):
        concept = country_source_export.get_concept_by_uri(nonversioned_uri)
        if concept:
            return concept['version_url']
        return ''

    def get_mapping_uri_from_export(self, country_source_export, from_concept_uri, map_type, to_concept_uri):
        mappings = country_source_export.get_mappings(
            from_concept_uri=from_concept_uri, to_concept_uri=to_concept_uri, map_type=map_type)
        if len(mappings) == 1:
            return mappings[0]['url']
        elif len(mappings) > 1:
            self.log('ERROR: More than one mapping returned')
        return ''
