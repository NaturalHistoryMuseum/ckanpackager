import re
from collections import OrderedDict

class DwcArchiveStructure(object):
    """Class representing the structure of a darwin core archive"""
    def __init__(self):
        self._extensions = OrderedDict()

    def add_extension(self, name):
        """Add a new extension to the archive structure

        @param name: The name of the extension
        """
        if name not in self._extensions:
            self._extensions[name] = OrderedDict()

    def add_term(self, input_field, ext_field, extension, term_name, formatter):
        """Add a new term to the archive structure

        @param input_field: The input field matching this term
        @param ext_field: If the input field is a json object, the field within
                          that
        @param extension: The extension in the archive structure
        @param term_name: The field name in the archive structure
        @param formatter: The formatter for this field, if there is one, if not this will be None
        """
        self.add_extension(extension)
        if term_name not in self._extensions[extension]:
            self._extensions[extension][term_name] = []
        if (input_field, ext_field, formatter) not in self._extensions[extension][term_name]:
            self._extensions[extension][term_name].append((input_field, ext_field, formatter))

    def extensions(self):
        """Return the list of extensions

        @returns: List of extensions
        @rtype: list
        """
        return self._extensions.keys()

    def terms(self, extension):
        """Return the list of terms in a given extension

        @param extension: Name of the extension
        @returns: List of terms
        @rtype: list
        """
        return self._extensions[extension].keys()

    def term_fields(self, extension, term):
        """Return the list of input fields that make up a term

        @param extension: Extension name
        @param term: Term name
        @rtype: list of (input field, extended field)
        """
        return self._extensions[extension][term]

    def file_name(self, extension):
        """Return the file name for a given extension

        @param extension: The extension name
        @returns: The file name
        """
        return self._uncamel_case(extension) + '.csv'

    def _uncamel_case(self, string, space='_'):
        """Given a camel cased, return a un-camel cased string

        Handles both lower and upper camel case.

        @param string: The string to un-camelcase
        @param space: The character used to separate words
        """
        matches = re.findall('(([A-Z]+|^)([^A-Z]+|$))', string)
        words = [m[0].lower() for m in matches if m]
        return space.join(words)
