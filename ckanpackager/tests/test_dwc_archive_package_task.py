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
from collections import OrderedDict
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
            'DWC_CORE_EXTENSION': os.path.join(
              os.path.join(
                  os.path.dirname(__file__),
                  '../../deployment/gbif_dwca_extensions/core/dwc_occurrence.xml'
              )
            ),
            'DWC_ADDITIONAL_EXTENSIONS': [os.path.join(
                os.path.dirname(__file__),
                '../../deployment/gbif_dwca_extensions/extensions/measurements_or_facts.xml'
            )],
            'DWC_ID_FIELD': '_id',
            'DWC_DYNAMIC_TERM': 'dynamicProperties',
            'DWC_EXTENSION_FIELDS': {
                'associatedMedia': {
                    'extension': os.path.join(
                        os.path.dirname(__file__),
                        '../../deployment/gbif_dwca_extensions/extensions/multimedia.xml'
                    ),
                    'fields': OrderedDict(
                        type='assocmed default type',
                        format='assocmed default format'
                    )
                }
            }
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
                    {'id': 'lesserKnownField'},
                    {'id': 'associatedMedia'}
                ],
                'records': [
                    {
                        '_id': '1',
                        'type': 'row 1 type',
                        'basisOfRecord': 'row 1 basis of record',
                        'Event date': 'row 1 event date',
                        'measurementRemarks': 'row 1 measurement remarks',
                        'unknownField': 'row 1 unknown field',
                        'lesserKnownField': 'row 1 lesser known field',
                        'associatedMedia': """[
                            {
                                "type": "row 1 assocmed type 1",
                                "format": "row 1 assocmed format 1"
                            },
                            {
                                "format": "row 1 assocmed format 2",
                                "type": "row 1 assocmed type 2"
                            },
                            {}
                        ]"""
                    },
                    {
                        '_id': '2',
                        'type': 'row 2 type',
                        'basisOfRecord': 'row 2 basis of record',
                        'Event date': 'row 2 event date',
                        'measurementRemarks': 'row 2 measurement remarks',
                        'unknownField': 'row 2 unknown field',
                        'lesserKnownField': 'row 2 lesser known field',
                        'associatedMedia': """{
                            "type": "row 2 assocmed type 1",
                            "format": "row 2 assocmed format 1"
                        }"""
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
        assert_equals(set(r.csv_files.keys()), set([
            'occurrence.csv',
            'measurement_or_fact.csv',
            'multimedia.csv'
        ]))

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
        assert_equals(r.csv_files['multimedia.csv'][0], [
            '_id', 'type', 'format'
        ])

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
    def test_core_extension_values(self):
        """ Ensure we get the expected values for the core extension """
        self._register_uri()
        r = DummyResource()
        self._task.create_zip(r)
        assert_equals(3, len(r.csv_files['occurrence.csv']))
        assert_equals(r.csv_files['occurrence.csv'][1][:4], [
            '1', 'row 1 type', 'row 1 basis of record', 'row 1 event date'
        ])
        assert_equals(json.loads(r.csv_files['occurrence.csv'][1][4]), {
            'unknownfield': 'row 1 unknown field',
            'lesserknownfield': 'row 1 lesser known field'
        })
        assert_equals(r.csv_files['occurrence.csv'][2][:4], [
            '2', 'row 2 type', 'row 2 basis of record', 'row 2 event date'
        ])
        assert_equals(json.loads(r.csv_files['occurrence.csv'][2][4]), {
            'unknownfield': 'row 2 unknown field',
            'lesserknownfield': 'row 2 lesser known field'
        })

    @httpretty.activate
    def test_additional_extension_values(self):
        """ Ensure we get the expected values for the additional extension """
        self._register_uri()
        r = DummyResource()
        self._task.create_zip(r)
        assert_equals(3, len(r.csv_files['measurement_or_fact.csv']))
        assert_equals(r.csv_files['measurement_or_fact.csv'][1], [
            '1', 'row 1 measurement remarks'
        ])
        assert_equals(r.csv_files['measurement_or_fact.csv'][2][:4], [
            '2', 'row 2 measurement remarks'
        ])

    @httpretty.activate
    def test_field_extension_values(self):
        """ Ensure we get the expected values for the field extensions """
        self._register_uri()
        r = DummyResource()
        self._task.create_zip(r)
        assert_equals(5, len(r.csv_files['multimedia.csv']))
        assert_equals(r.csv_files['multimedia.csv'][1], [
            '1', 'row 1 assocmed type 1', 'row 1 assocmed format 1'
        ])
        assert_equals(r.csv_files['multimedia.csv'][2], [
            '1', 'row 1 assocmed type 2', 'row 1 assocmed format 2'
        ])
        assert_equals(r.csv_files['multimedia.csv'][3], [
            '1', 'assocmed default type', 'assocmed default format'
        ])
        assert_equals(r.csv_files['multimedia.csv'][4], [
            '2', 'row 2 assocmed type 1', 'row 2 assocmed format 1'
        ])

    @httpretty.activate
    def test_correct_meta_xml(self):
        """Check we get the correct meta.xml"""
        #TODO: Parse the XML for more fine-grained tests
        self._register_uri()
        r = DummyResource()
        self._task.create_zip(r)
        assert_equals(
            re.sub('\s+', '', r.text_files['meta.xml'].getvalue()),
            re.sub('\s+', '', """<archive xmlns="http://rs.tdwg.org/dwc/text/">
  <core encoding="UTF-8" linesTerminatedBy="\\n" fieldsTerminatedBy="," fieldsEnclosedBy="&quot;" ignoreHeaderLines="1" rowType="http://rs.tdwg.org/dwc/terms/Occurrence">
    <files>
      <location>occurrence.csv</location>
    </files>
    <id index="0"/>
    <field index="1" term="http://purl.org/dc/terms/type"/>
    <field index="2" term="http://rs.tdwg.org/dwc/terms/basisOfRecord"/>
    <field index="3" term="http://rs.tdwg.org/dwc/terms/eventDate"/>
    <field index="4" term="http://rs.tdwg.org/dwc/terms/dynamicProperties"/>
  </core>
  <extension encoding="UTF-8" linesTerminatedBy="\\n" fieldsTerminatedBy="," fieldsEnclosedBy="&quot;" ignoreHeaderLines="1" rowType="http://rs.tdwg.org/dwc/terms/MeasurementOrFact">
    <files>
      <location>measurement_or_fact.csv</location>
    </files>
    <coreid index="0"/>
    <field index="1" term="http://rs.tdwg.org/dwc/terms/measurementRemarks"/>
  </extension>
  <extension encoding="UTF-8" linesTerminatedBy="\\n" fieldsTerminatedBy="," fieldsEnclosedBy="&quot;" ignoreHeaderLines="1" rowType="http://rs.gbif.org/terms/1.0/Multimedia">
    <files>
      <location>multimedia.csv</location>
    </files>
    <coreid index="0"/>
    <field index="1" term="http://purl.org/dc/terms/type"/>
    <field index="2" term="http://purl.org/dc/terms/format"/>
  </extension>
</archive>"""))
