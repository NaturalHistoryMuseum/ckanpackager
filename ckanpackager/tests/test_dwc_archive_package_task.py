"""Test the DwcArchivePackageTask

Note that as DwcArchivePackageTask inherits from DatastorePackageTasks, we
do not test the base functionality here.

TODO: Fake GBIFDarwinCoreMapping
"""
import re
import json
import StringIO
import os
import httpretty
from nose.tools import assert_equals, assert_in, assert_not_in
from ckanpackager.tasks.dwc_archive_package_task import DwcArchivePackageTask


class FakeCSVWriter(object):
    """ A Fake CSV Writer"""
    def __init__(self, rows):
        self.rows = rows

    def writerow(self, row):
        self.rows.append(row)


class DummyResource(object):
    """Fake Resourcefile object """
    def __init__(self):
        self.csv_files = {}
        self.text_files = {}
        self.create_invoked = False
        self.clean_invoked = False

    def get_csv_writer(self, file_name):
        if file_name not in self.csv_files:
            self.csv_files[file_name] = []
        return FakeCSVWriter(self.csv_files[file_name])

    def get_writer(self, file_name):
        if file_name not in self.text_files:
            self.text_files[file_name] = StringIO.StringIO()
        return self.text_files[file_name]

    def create_zip(self, command):
        self.create_invoked = True

    def clean_work_files(self):
        self.clean_invoked = True

class TestDwcArchivePackageTask(object):
    def setUp(self):
        """Setup up test config&folders"""
        self._config = {
            'ZIP_COMMAND': "/usr/bin/zip -j {output} {input}",
            'PAGE_SIZE': 5,
            'DWC_EXTENSION_PATHS': [
                os.path.join(
                    os.path.dirname(__file__),
                    '../../deployment/gbif_dwca_extensions/core/dwc_occurrence.xml'
                ),
                os.path.join(
                    os.path.dirname(__file__),
                    '../../deployment/gbif_dwca_extensions/extensions/measurements_or_facts.xml'
                )
            ],
            'DWC_ID_FIELD': '_id',
            'DWC_DYNAMIC_TERM': 'dynamicProperties'
        }
        self._task = DwcArchivePackageTask({
            'resource_id': 'the-resource-id',
            'email': 'someone@0.0.0.0',
            'api_url': 'http://example.com/datastore/search'
        }, self._config)

    def _ckan_response(self, request, uri, headers):
        data = {
            'result': {
                'fields': [
                    {'id': '_id'},
                    {'id': 'type'},
                    {'id': 'basisOfRecord'},
                    {'id': 'Event date'},
                    {'id': 'measurementRemarks'},
                    {'id': 'unknownField'},
                    {'id': 'lesserKnownField'}
                ],
                'records': [
                    {
                        '_id': '1',
                        'type': 'row 1 type',
                        'basisOfRecord': 'row 1 basis of record',
                        'Event date': 'row 1 event date',
                        'measurementRemarks': 'row 1 measurement remarks',
                        'unknownField': 'row 1 unknown field',
                        'lesserKnownField': 'row 1 lesser known field'
                    },
                    {
                        '_id': '2',
                        'type': 'row 2 type',
                        'basisOfRecord': 'row 2 basis of record',
                        'Event date': 'row 2 event date',
                        'measurementRemarks': 'row 2 measurement remarks',
                        'unknownField': 'row 2 unknown field',
                        'lesserKnownField': 'row 2 lesser known field'
                    }
                ]
            }
        }
        return 200, headers, json.dumps(data)

    def _register_uri(self):
        """Register the httpretty URI with fake data"""
        httpretty.register_uri(
            httpretty.POST,
            'http://example.com/datastore/search',
            body=self._ckan_response
        )

    @httpretty.activate
    def test_expected_extensions(self):
        """Ensure that the expected extensions are in the output"""
        self._register_uri()
        r = DummyResource()
        self._task.create_zip(r)
        assert_equals(set(r.csv_files.keys()), set(['occurrence.csv', 'measurement_or_fact.csv']))

    @httpretty.activate
    def test_camelcase_field_mapping(self):
        """Ensure that camel-cased input fields are mapped to the correct DwC
        field."""
        self._register_uri()
        r = DummyResource()
        self._task.create_zip(r)
        assert_equals(r.csv_files['occurrence.csv'][0][2], 'basisOfRecord')

    @httpretty.activate
    def test_space_field_mapping(self):
        """Ensure that space separated input fields are mapped to the correct DwC
        field."""
        self._register_uri()
        r = DummyResource()
        self._task.create_zip(r)
        assert_equals(r.csv_files['occurrence.csv'][0][3], 'eventDate')

    @httpretty.activate
    def test_fields_in_correct_extension(self):
        """Ensure that the fields are in the correct extension"""
        self._register_uri()
        r = DummyResource()
        self._task.create_zip(r)
        assert_equals(r.csv_files['occurrence.csv'][0], [
            '_id', 'type', 'basisOfRecord', 'eventDate', 'dynamicProperties'
        ])
        assert_equals(r.csv_files['measurement_or_fact.csv'][0], ['_id', 'measurementRemarks'])

    @httpretty.activate
    def test_unknown_fields_in_dynamic_properties(self):
        """Ensure that unknown fields are mapped to dynamic properties, and that
        the content of the row contains the expected value pairs"""
        self._register_uri()
        r = DummyResource()
        self._task.create_zip(r)
        assert_in('dynamicProperties', r.csv_files['occurrence.csv'][0])
        assert_not_in('unknownField',r.csv_files['occurrence.csv'][0])
        assert_not_in('lesserKnownField',r.csv_files['occurrence.csv'][0])
        assert_equals(r.csv_files['occurrence.csv'][1][4], '{"lesserknownfield": "row 1 lesser known field", "unknownfield": "row 1 unknown field"}')

    @httpretty.activate
    def test_correct_meta_xml(self):
        """Check we get the correct meta.xml"""
        #TODO: Parse the XML for more fine-grained tests
        self._register_uri()
        r = DummyResource()
        self._task.create_zip(r)
        assert_equals(
            re.sub('\s+', '', r.text_files['meta.xml'].getvalue()),
            re.sub('\s+', '', '<archive xmlns="http://rs.tdwg.org/dwc/text/">\n  <core encoding="UTF-8" linesTerminatedBy="\\n" fieldsTerminatedBy="," fieldsEnclosedBy="&quot;" ignoreHeaderLines="1" rowType="http://rs.tdwg.org/dwc/terms/Occurrence">\n    <files>\n      <location>occurrence.csv</location>\n    </files>\n    <id index="0"/>\n    <field index="1" term="http://purl.org/dc/terms/type"/>\n    <field index="2" term="http://rs.tdwg.org/dwc/terms/basisOfRecord"/>\n    <field index="3" term="http://rs.tdwg.org/dwc/terms/eventDate"/>\n    <field index="4" term="http://rs.tdwg.org/dwc/terms/dynamicProperties"/>\n  </core>\n  <extension encoding="UTF-8" linesTerminatedBy="\\n" fieldsTerminatedBy="," fieldsEnclosedBy="&quot;" ignoreHeaderLines="1" rowType="http://rs.tdwg.org/dwc/terms/MeasurementOrFact">\n    <files>\n      <location>measurement_or_fact.csv</location>\n    </files>\n    <coreid index="0"/>\n    <field index="1" term="http://rs.tdwg.org/dwc/terms/measurementRemarks"/>\n  </extension>\n</archive>\n')
        )
