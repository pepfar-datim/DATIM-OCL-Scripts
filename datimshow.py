from datimbase import DatimBase


class DatimShow(DatimBase):

    # Presentation Formats
    DATIM_FORMAT_HTML = 'html'
    DATIM_FORMAT_XML = 'xml'
    DATIM_FORMAT_JSON = 'json'
    DATIM_FORMAT_CSV = 'csv'
    PRESENTATION_FORMATS = [
        DATIM_FORMAT_HTML,
        DATIM_FORMAT_XML,
        DATIM_FORMAT_JSON,
        DATIM_FORMAT_CSV
    ]

    def __init__(self):
        DatimBase.__init__(self)
