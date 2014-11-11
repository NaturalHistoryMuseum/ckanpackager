from collections import OrderedDict
from lxml import etree

_extensions = {}


class GBIFDarwinCoreMapping(object):
    def __init__(self, extension_paths, reset=False):
        """Class used to represent the mapping from Darwin Core terms
        to a GBIF compatible list of Darwin Core Archive extensions and
        terms.

        The list is based on one core extension and a list of additional
        extensions. The fields allowed in each extension are defined
        using GBIF provided XML files.

        Extensions are defined by the schema at
        http://rs.gbif.org/schema/extension.xsd . The Occurrence core extension
        can be found at:
        http://rs.gbif.org/core/dwc_occurrence.xml
        While other GBIF supported extensions are available at:
        http://rs.gbif.org/extension/

        @param extension_paths: List of paths to the extension definition XML.
                                The first listed extension will be assumed
                                to be the core extension.
        @param reset: If True, ignore cached version and parse anew.
        """
        self._terms = {}
        self._extensions = OrderedDict()
        self._core_extension = None
        for extension_path in extension_paths:
            extension = self._parse_extension(extension_path, reset)
            self._extensions[extension['name']] = extension
            if self._core_extension is None:
                self._core_extension = extension
        for extension in self._extensions:
            for term in self._extensions[extension]['terms']:
                # Don't overwrite. We don't accept duplicate terms, and if
                # conflict existed we'd want to prefer the core extension.
                if term['name'] not in self._terms:
                    self._terms[term['name']] = term

    def extensions(self):
        """Return the list of extension names. The core extension is always
           first.
        """
        return self._extensions.keys()

    def row_type(self, extension):
        """Return the row type of a given extension"""
        return self._extensions[extension]['row_type']

    def terms(self, extension):
        """Return the term names of a given extension"""
        terms = []
        for term in self._extensions[extension]['terms']:
            terms.append(term['name'])
        return terms

    def is_core_extension(self, extension):
        """Returns True if the given extension is the core extension"""
        return self._core_extension['name'] == extension

    def term_extension(self, term):
        """Return the extension name of a given term"""
        return self._terms[term]['extension']

    def term_qualified_name(self, term):
        """Return the qualified name of a given term"""
        return self._terms[term]['qualified']

    def term_exists(self, term):
        """Return True if the term exists"""
        return term in self._terms

    def _parse_extension(self, extension_path, reset=False):
        """Parse a GBIF DwC XML extension file and return it

        @param extension_path: Path to the XML file
        @param reset: If True, clear cached version and re-parse
        @returns: Definition of an extension, as a dict defining 'name',
                  'row_type' and 'terms'.
        """
        global _extensions
        if reset or extension_path not in _extensions:
            xml_tree = etree.parse(extension_path)
            xml_root = xml_tree.getroot()
            extension_name = xml_root.get('name')
            _extensions[extension_path] = {
                'name': extension_name,
                'row_type': xml_root.get('rowType'),
                'terms': []
            }
            namespace = ''
            if None in xml_root.nsmap:
                namespace = '{' + xml_root.nsmap[None] + '}'
            for xml_property in xml_root.findall(namespace + 'property'):
                name = xml_property.get('name')
                _extensions[extension_path]['terms'].append({
                    'name': name,
                    'extension': extension_name,
                    'qualified': xml_property.get('qualName'),
                    'required': xml_property.get('required') == 'true'
                })
        return _extensions[extension_path]