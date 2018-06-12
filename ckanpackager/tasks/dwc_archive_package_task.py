import json
import ijson
from decimal import Decimal
from lxml import etree
from uuid import uuid4
from time import strftime, gmtime
from ckanpackager.lib.gbif_darwincore_mapping import GBIFDarwinCoreMapping
from ckanpackager.lib.dwc_archive_structure import DwcArchiveStructure
from ckanpackager.tasks.datastore_package_task import DatastorePackageTask


class DwcArchivePackageTask(DatastorePackageTask):
    def __init__(self, *args):
        super(DwcArchivePackageTask, self).__init__(*args)
        self._dwc_core_terms = GBIFDarwinCoreMapping(
            [self.config['DWC_CORE_EXTENSION']]
            + self.config['DWC_ADDITIONAL_EXTENSIONS']
        )
        self._dwc_extension_fields = {}
        for field_name in self.config['DWC_EXTENSION_FIELDS']:
            self._dwc_extension_fields[field_name] = GBIFDarwinCoreMapping(
                [self.config['DWC_EXTENSION_FIELDS'][field_name]['extension']]
            )

    def schema(self):
        """Define the schema for datastore package tasks

        Each field is a tuple defining (required, processing function, forward to ckan)
        """
        schema = super(DwcArchivePackageTask, self).schema()
        schema['eml'] = (False, None, False)
        return schema

    def _write_headers(self, response, resource):
        """Stream the list of fields and save the headers in the resource.

        We construct the structure of the darwin core archive at the same
        time.

        @param input_stream: file-like object representing the input stream
        @param resource: A resource file
        @type resource: ResourceFile
        @returns: List (or other structure) representing the fields saved to
                  the headers, to allow _stream_records to save rows in a
                  matching format.
        """
        # Create the structure
        archive = DwcArchiveStructure()
        for field in response['result']['fields']:
            if field['id'] != self.config['DWC_ID_FIELD']:
                # A single input field can match into multiple destination
                # term (when using json extension fields). Also multiple
                # input fields can match into the same destination term
                # (when using dynamic terms)
                extensions = self._input_field_to_extension(field['id'])
                for ext_field, extension, term, formatter in extensions:
                    archive.add_term(field['id'], ext_field, extension, term, formatter)

        # Now prepare all the files we will need
        for extension in archive.extensions():
            terms = archive.terms(extension)
            terms.insert(0, self.config['DWC_ID_FIELD'])
            w = resource.get_csv_writer(archive.file_name(extension))
            w.writerow(terms)
        return archive

    def _write_records(self, records, archive, resource):
        """Write the records from the search response to the resource files

        @param records: list of records
        @type archive: DwcArchiveStructure
        @type resource: ResourceFile
        @returns: Number of rows read
        """
        def no_decimal(x):
            if isinstance(x, Decimal):
                return float(x)
            else:
                return x

        for record in records:
            json_row = dict([(k, no_decimal(v)) for (k, v) in record.items()])
            for extension in archive.extensions():
                w = resource.get_csv_writer(archive.file_name(extension))
                # Get field/values relevant for this extension, and expand
                # extended fields into multiple rows if needed.
                ext_row = self._row_for_extension(archive, extension, json_row)
                if len(ext_row) == 0:
                    continue
                ext_count = len(ext_row.values()[0])
                for index in range(ext_count):
                    row = [ext_row[self.config['DWC_ID_FIELD']][index]]
                    for term in archive.terms(extension):
                        # Get all the input fields that go into this output field
                        term_fields = archive.term_fields(extension, term)
                        values = {}
                        for (term_field, ext_term_field, formatter) in term_fields:
                            if ext_term_field:
                                inner_name = "{}_{}".format(term_field, ext_term_field)
                                j_val = ext_row[term_field][index]
                                if j_val:
                                    value = j_val.get(ext_term_field, None)
                                    # apply a formatter to the value if there is one
                                    values[inner_name] = formatter(value) if formatter else value
                                else:
                                    values[inner_name] = None
                            else:
                                values[term_field] = ext_row[term_field][index]
                        # Output value or json if multiple values map.
                        if len(values) == 1:
                            row.append(values.values()[0])
                        else:
                            combined = {}
                            for term_field, value in values.items():
                                if value is None:
                                    continue
                                try:
                                    # We are doing JSON anyway, we might as
                                    # well un-json sub-values.
                                    value = json.loads(value)
                                except (ValueError, TypeError):
                                    pass
                                cc_field = self._camel_case(term_field)
                                combined[cc_field] = value
                            row.append(json.dumps(combined, ensure_ascii=False).encode('utf8'))
                    w.writerow(row)

    def _finalize_resource(self, archive, resource):
        """Finalize the resource before ZIPing it.

        @type archive: DwcArchiveStructure
        @type resource: ResourceFile
        """
        x_meta = etree.Element('archive')
        x_meta.attrib['xmlns'] = 'http://rs.tdwg.org/dwc/text/'
        if 'eml' in self.request_params:
            x_meta.attrib['metadata'] = 'eml.xml'
        for extension in archive.extensions():
            if self._dwc_core_terms.is_core_extension(extension):
                x_section = etree.SubElement(x_meta, 'core')
            else:
                x_section = etree.SubElement(x_meta, 'extension')
            terms = self._dwc_core_terms
            if not terms.has_extension(extension):
                for field_name, extension_terms in self._dwc_extension_fields.items():
                    if extension_terms.has_extension(extension):
                        terms = extension_terms
            x_section.attrib['encoding'] = 'UTF-8'
            x_section.attrib['linesTerminatedBy'] = "\\n"
            x_section.attrib['fieldsTerminatedBy'] = ","
            x_section.attrib['fieldsEnclosedBy'] = '"'
            x_section.attrib['ignoreHeaderLines'] = str(1)
            x_section.attrib['rowType'] = terms.row_type(extension)
            x_files = etree.SubElement(x_section, 'files')
            x_location = etree.SubElement(x_files, 'location')
            x_location.text = archive.file_name(extension)
            if self._dwc_core_terms.is_core_extension(extension):
                x_id = etree.SubElement(x_section, 'id')
            else:
                x_id = etree.SubElement(x_section, 'coreid')
            x_id.attrib['index'] = str(0)
            for (index, term) in enumerate(archive.terms(extension)):
                x_field = etree.SubElement(x_section, 'field')
                x_field.attrib['index'] = str(index + 1)
                x_field.attrib['term'] = terms.term_qualified_name(term)
        meta_writer = resource.get_writer('meta.xml')
        meta_writer.write(etree.tostring(x_meta, pretty_print=True))
        if 'eml' in self.request_params:
            eml_writer = resource.get_writer('eml.xml')
            eml_writer.write(self.request_params['eml'].format(
              package_id=uuid4(),
              pub_date=strftime('%Y-%m-%d'),
              date_stamp=strftime('%Y-%m-%dT%H:%M:%S+0000', gmtime())
            ))

    def _row_for_extension(self, archive, extension, json_row):
        """ Return an input row with the fields relevant to an extension

        This ensures that:
        - The configured ID field is included;
        - All values are a list, and all lists have the same length (the last
          value is repeated as many times as needed);
        - All fields in the extension exist (with a value of None if the
          equivalent field wasn't in the input data)
        - Extension field's json is decoded and default values inserted.

        @param archive: The DwcArchiveStructure object
        @param extension: The extension
        @param json_row: The input row
        @returns: A row with the appropriate fields as lists and unjsonned.
        """
        id_field = self.config['DWC_ID_FIELD']
        result = {
            id_field: [json_row.get(id_field, None)]
        }
        seen = [(id_field, None)]
        max_row = 0
        # Starting from the destination term, find all the required source
        # fields/values. Decode JSON values for extended term fields.
        for term in archive.terms(extension):
            term_fields = archive.term_fields(extension, term)
            for (term_field, ext_term_field, formatter) in term_fields:
                if (term_field, ext_term_field, formatter) in seen:
                    continue
                seen.append((term_field, ext_term_field, formatter))
                if ext_term_field:
                    try:
                        result[term_field] = json.loads(json_row[term_field])
                        if not isinstance(result[term_field], list):
                            result[term_field] = [result[term_field]]
                    except (ValueError, KeyError, TypeError):
                        result[term_field] = []
                    for (index, value) in enumerate(result[term_field]):
                        result[term_field][index] = dict(
                            self.config['DWC_EXTENSION_FIELDS'][term_field]['fields'].items()
                            + value.items()
                        )
                elif term_field in json_row:
                    result[term_field] = [json_row[term_field]]
                else:
                    result[term_field] = [None]

                if len(result[term_field]) > max_row:
                    max_row = len(result[term_field])

        # Ensure all are the same length by repeating the last value
        for term_field in result:
            l = len(result[term_field])
            if l == max_row:
                continue
            if l > 0:
                v = result[term_field][l-1]
            else:
                v = None
            for i in range(max_row - l):
                result[term_field].append(v)
        return result

    def _input_field_to_extension(self, field, terms=None):
        """Return the list of DwC extension and field name tuples
        corresponding to the given input field name.

        - If the field is an extension field, then this will return a list
          for each of the configured fields in the extension, defining which
          subfield matches. eg, for for input field associatedMedia we'll get:
          [('ckan_type', 'multimedia', 'type'),
           ('ckan_title', 'multimedia', 'title'),
           ...]
        - If the field is not an extension field, then return the extension
          and matching dwc term tuple as a single element in an array (with an
          empty extension sub field), eg:
          [(None, 'occurrence', 'catalogueNumber')]
        - If the field is not in any extension, then assume it is part of the
          defined dynamic properties, eg.:
          [(None, 'occurrence', 'dynamicProperties')]

        @param field: The input field name
        @param terms: The GBIFDarwinCoreMapping object to use. If None,
                      will use the core extension object.
        @returns: [(extension sub field, extension, dwc term)]
        @rtype: list
        """
        # Handle extension fields
        if terms is None and field in self._dwc_extension_fields:
            result = []
            extension_fields = self.config['DWC_EXTENSION_FIELDS'][field]['fields'].keys()
            for extension_field in extension_fields:
                sub_result = self._input_field_to_extension(
                    extension_field,
                    self._dwc_extension_fields[field]
                )
                # retrieve any mappings from term name to field name, defaults to the extension_field
                ext_name = self.config['DWC_EXTENSION_FIELDS'][field]['mappings'].get(extension_field, extension_field)
                # retrieve any formatters for the term name, defaulting to None
                formatter = self.config['DWC_EXTENSION_FIELDS'][field]['formatters'].get(extension_field, None)
                result = result + [(ext_name, v[1], v[2], formatter) for v in sub_result]
            return result

        # Handle core fields
        if terms is None:
            terms = self._dwc_core_terms
            dynamic_term = self.config['DWC_DYNAMIC_TERM']
        else:
            dynamic_term = None

        if terms.term_exists(field):
            term = field
        else:
            term = self._camel_case(field)
            if not terms.term_exists(term):
                term = dynamic_term
        if term:
            extension = terms.term_extension(term)
            return [(None, extension, term, None)]
        else:
            return []

    def _camel_case(self, string):
        """Camel case the given space-separated string

        - This uses lower camel case;
        - words that all in capitals are kept all in capitals.

        so 'Taxon resource ID' becomes 'taxonResourceID'

        @param string: The string to camel case
        @returns: The un-camel cased field
        """
        words = [w.strip() for w in string.split(' ') if w.strip()]
        if len(words) == 0:
            return ''
        if words[0].upper() != words[0]:
            words[0] = words[0].lower()
        for i, w in enumerate(words[1:]):
            if words[i+1].upper() != words[i+1]:
                words[i+1] = words[i+1].capitalize()
        return "".join(words)
