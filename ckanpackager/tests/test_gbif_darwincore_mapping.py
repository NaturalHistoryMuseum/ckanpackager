import os
from nose.tools import assert_equals, assert_true, assert_false
from ckanpackager.lib.gbif_darwincore_mapping import GBIFDarwinCoreMapping

class TestGBIFDarwinCoreMapping(object):
    def setUp(self):
        """Set the test files to be used"""
        self.paths = [
            os.path.join(
                os.path.dirname(__file__),
                '../../deployment/gbif_dwca_extensions',
                f
            )
            for f in ['core/dwc_occurrence.xml', 'extensions/measurements_or_facts.xml']
        ]

    def test_expected_extensions(self):
        """Ensure we found the extensions we expect in the test files"""
        dwc_terms = GBIFDarwinCoreMapping(self.paths)
        assert_equals(set(dwc_terms.extensions()), set([
            'Occurrence', 'MeasurementOrFact'
        ]))

    def test_is_core_extension(self):
        """ Ensure the first extension is set as core extension """
        dwc_terms = GBIFDarwinCoreMapping(self.paths)
        assert_true(dwc_terms.is_core_extension('Occurrence'))
        assert_false(dwc_terms.is_core_extension('MeasurementOrFact'))
        assert_false(dwc_terms.is_core_extension('ImNotReallyHere'))

    def test_has_extension(self):
        """ Ensure we can check whether an extension is present """
        dwc_terms = GBIFDarwinCoreMapping(self.paths)
        assert_true(dwc_terms.has_extension('Occurrence'))
        assert_true(dwc_terms.has_extension('MeasurementOrFact'))
        assert_false(dwc_terms.has_extension('ImNotReallyHere'))

    def test_expected_terms_in_occurrence(self):
        """Ensure we found the expected terms in Occurrence extension
        """
        dwc_terms = GBIFDarwinCoreMapping(self.paths)
        assert_equals(set(dwc_terms.terms('Occurrence')), set([
            'identificationRemarks', 'minimumDepthInMeters', 'footprintSRS',
            'verbatimLatitude', 'month', 'dataGeneralizations',
            'lithostratigraphicTerms', 'latestPeriodOrHighestSystem',
            'reproductiveCondition', 'continent', 'endDayOfYear',
            'identificationID', 'occurrenceID', 'locationAccordingTo',
            'latestEpochOrHighestSeries', 'coordinateUncertaintyInMeters',
            'coordinatePrecision', 'source', 'maximumDepthInMeters',
            'waterBody', 'kingdom', 'decimalLatitude', 'verbatimTaxonRank',
            'earliestEraOrLowestErathem', 'municipality',
            'acceptedNameUsageID', 'infraspecificEpithet', 'namePublishedIn',
            'nameAccordingToID', 'informationWithheld', 'nomenclaturalStatus',
            'latestEraOrHighestErathem', 'recordNumber', 'day',
            'individualCount', 'institutionID',
            'georeferenceVerificationStatus', 'lifeStage',
            'associatedSequences', 'scientificName', 'parentNameUsage',
            'datasetID', 'eventID', 'lowestBiostratigraphicZone', 'habitat',
            'higherGeographyID', 'minimumElevationInMeters', 'sex', 'member',
            'associatedTaxa', 'year', 'materialSampleID', 'taxonRemarks',
            'namePublishedInYear', 'identificationVerificationStatus',
            'eventTime', 'basisOfRecord', 'latestEonOrHighestEonothem',
            'otherCatalogNumbers', 'acceptedNameUsage', 'georeferenceSources',
            'specificEpithet', 'verbatimLocality', 'identificationReferences',
            'behavior', 'geodeticDatum', 'occurrenceRemarks', 'collectionCode',
            'higherGeography', 'nameAccordingTo', 'latestAgeOrHighestStage',
            'fieldNumber', 'disposition', 'earliestEpochOrLowestSeries',
            'group', 'highestBiostratigraphicZone', 'accessRights',
            'ownerInstitutionCode', 'occurrenceDetails',
            'bibliographicCitation', 'scientificNameID',
            'earliestAgeOrLowestStage', 'language', 'island',
            'decimalLongitude', 'locationID', 'startDayOfYear', 'formation',
            'genus', 'family', 'collectionID', 'dynamicProperties',
            'eventRemarks', 'verbatimCoordinates', 'individualID',
            'footprintWKT', 'county', 'associatedMedia', 'locationRemarks',
            'references', 'pointRadiusSpatialFit', 'footprintSpatialFit',
            'recordedBy', 'higherClassification', 'islandGroup', 'eventDate',
            'verbatimSRS', 'associatedOccurrences', 'catalogNumber',
            'verbatimLongitude', 'type', 'preparations', 'taxonID',
            'nomenclaturalCode', 'maximumElevationInMeters',
            'verbatimCoordinateSystem', 'datasetName',
            'earliestEonOrLowestEonothem', 'rights', 'verbatimDepth',
            'modified', 'bed', 'georeferencedDate', 'georeferencedBy',
            'country', 'parentNameUsageID', 'georeferenceRemarks',
            'occurrenceStatus', 'vernacularName', 'subgenus', 'countryCode',
            'phylum', 'institutionCode', 'rightsHolder',
            'identificationQualifier', 'namePublishedInID', 'identifiedBy',
            'earliestPeriodOrLowestSystem',
            'minimumDistanceAboveSurfaceInMeters',
            'maximumDistanceAboveSurfaceInMeters', 'taxonConceptID',
            'georeferenceProtocol', 'locality', 'associatedReferences',
            'stateProvince', 'taxonomicStatus', 'originalNameUsage',
            'taxonRank', 'previousIdentifications', 'samplingEffort',
            'verbatimElevation', 'establishmentMeans', 'typeStatus',
            'samplingProtocol', 'originalNameUsageID', 'class',
            'geologicalContextID', 'fieldNotes', 'dateIdentified',
            'verbatimEventDate', 'scientificNameAuthorship', 'order'
        ]))

    def test_expected_terms_in_measurement_or_fact(self):
        """Ensure we found the expected terms in MeasurementOrFact extension
        """
        dwc_terms = GBIFDarwinCoreMapping(self.paths)
        assert_equals(set(dwc_terms.terms('MeasurementOrFact')), set([
            'measurementRemarks', 'measurementAccuracy', 'measurementValue',
            'measurementDeterminedDate', 'measurementUnit', 'measurementID',
            'measurementDeterminedBy', 'measurementType', 'measurementMethod'
        ]))

    def test_expected_occurrence_row_type(self):
        """Ensure we get the expected rowtype for Occurrence extension

        Note we test this for one class only to check the functionality
        """
        dwc_terms = GBIFDarwinCoreMapping(self.paths)
        assert_equals(
            dwc_terms.row_type('Occurrence'),
            'http://rs.tdwg.org/dwc/terms/Occurrence'
        )

    def test_term_exists(self):
        """Test that term_exists works as expected"""
        dwc_terms = GBIFDarwinCoreMapping(self.paths)
        assert_true(dwc_terms.term_exists('lifeStage'))
        assert_true(dwc_terms.term_exists('measurementRemarks'))
        assert_false(dwc_terms.term_exists('somethingThatDoesntExist'))

    def test_term_group(self):
        """Test that term class works as expected"""
        dwc_terms = GBIFDarwinCoreMapping(self.paths)
        assert_equals('Occurrence', dwc_terms.term_extension('lifeStage'))
        assert_equals('MeasurementOrFact', dwc_terms.term_extension('measurementRemarks'))

    def test_term_qualified(self):
        """Test that term qualified name works as expected"""
        dwc_terms = GBIFDarwinCoreMapping(self.paths)
        assert_equals(
            'http://rs.tdwg.org/dwc/terms/lifeStage',
            dwc_terms.term_qualified_name('lifeStage')
        )
