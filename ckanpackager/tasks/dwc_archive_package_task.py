import json
import ijson
from decimal import Decimal
from lxml import etree
from ckanpackager.lib.gbif_darwincore_mapping import GBIFDarwinCoreMapping
from ckanpackager.lib.dwc_archive_structure import DwcArchiveStructure
from ckanpackager.tasks.datastore_package_task import DatastorePackageTask


class DwcArchivePackageTask(DatastorePackageTask):
    def __init__(self, *args):
        super(DwcArchivePackageTask, self).__init__(*args)
        self._dwc_terms = GBIFDarwinCoreMapping(self.config['DWC_EXTENSION_PATHS'])

    def _stream_headers(self, input_stream, resource):
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
        for field_id in ijson.items(input_stream, 'result.fields.item.id'):
            if field_id != self.config['DWC_ID_FIELD']:
                (extension, term) = self._field_to_dwc(field_id)
                archive.add_term(field_id, extension, term)

        # Now prepare all the files we will need
        for extension in archive.extensions():
            terms = archive.terms(extension)
            terms.insert(0, self.config['DWC_ID_FIELD'])
            w = resource.get_csv_writer(archive.file_name(extension))
            w.writerow(terms)
        return archive

    def _stream_records(self, input_stream, archive, resource):
        """Stream the records from the input stream to the resource files

        @param input_stream: file-like object representing the input stream
        @type archive: DwcArchiveStructure
        @type resource: ResourceFile
        @returns: Number of rows saved
        """
        saved = 0
        def no_decimal(x):
            if isinstance(x, Decimal):
                return float(x)
            else:
                return x
        for json_row in ijson.items(input_stream, 'result.records.item'):
            json_row = dict([(k, no_decimal(v)) for (k, v) in json_row.items()])
            for extension in archive.extensions():
                row = [json_row.get(self.config['DWC_ID_FIELD'], None)]
                for term in archive.terms(extension):
                    term_fields = archive.term_fields(extension, term)
                    if len(term_fields) == 1:
                        row.append(json_row.get(term_fields[0], None))
                    else:
                        combined = {}
                        for term_field in term_fields:
                            cc_field = self._camel_case(term_field)
                            combined[cc_field] = json_row.get(term_field, None)
                        row.append(json.dumps(combined))

                w = resource.get_csv_writer(archive.file_name(extension))
                w.writerow(row)
            saved += 1
        return saved

    def _finalize_resource(self, archive, resource):
        """Finalize the resource before ZIPing it.

        @type archive: DwcArchiveStructure
        @type resource: ResourceFile
        """
        x_meta = etree.Element('archive')
        x_meta.attrib['xmlns'] = 'http://rs.tdwg.org/dwc/text/'
        for extension in archive.extensions():
            if self._dwc_terms.is_core_extension(extension):
                x_section = etree.SubElement(x_meta, 'core')
            else:
                x_section = etree.SubElement(x_meta, 'extension')
            x_section.attrib['encoding'] = 'UTF-8'
            x_section.attrib['linesTerminatedBy'] = "\\n"
            x_section.attrib['fieldsTerminatedBy'] = ","
            x_section.attrib['fieldsEnclosedBy'] = '"'
            x_section.attrib['ignoreHeaderLines'] = str(1)
            x_section.attrib['rowType'] = self._dwc_terms.row_type(extension)
            x_files = etree.SubElement(x_section, 'files')
            x_location = etree.SubElement(x_files, 'location')
            x_location.text = archive.file_name(extension)
            if self._dwc_terms.is_core_extension(extension):
                x_id = etree.SubElement(x_section, 'id')
            else:
                x_id = etree.SubElement(x_section, 'coreid')
            x_id.attrib['index'] = str(0)
            for (index, term) in enumerate(archive.terms(extension)):
                x_field = etree.SubElement(x_section, 'field')
                x_field.attrib['index'] = str(index + 1)
                x_field.attrib['term'] = self._dwc_terms.term_qualified_name(term)
        meta_writer = resource.get_writer('meta.xml')
        meta_writer.write(etree.tostring(x_meta, pretty_print=True))

    def _field_to_dwc(self, field):
        """Return the DwC extension and field name corresponding to the given
        input field name.

        If the field is not a darwin core field, the dynamic term is returned
        instead.

        @param field: The input field name
        @returns: (dwc class, dwc field name) tuple
        @rtype: tuple
        """
        if self._dwc_terms.term_exists(field):
            cc_field = field
        else:
            cc_field = self._camel_case(field)
            if not self._dwc_terms.term_exists(cc_field):
                cc_field = self.config['DWC_DYNAMIC_TERM']
        extension = self._dwc_terms.term_extension(cc_field)
        return extension, cc_field

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


