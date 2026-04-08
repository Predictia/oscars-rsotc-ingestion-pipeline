import griffe


class ProvenanceDocstring:
    """
    Extracts provenance metadata from the docstrings (e.g. inputs, outputs) of the main class or method.

    Parameters
    ----------
    class_path : str
        Full path to the class to analyze.
    docstring_convention : str
        Docstring convention used (e.g., "google", "numpy").
    """

    def __init__(self, class_path: str, docstring_convention: str):
        self.class_path = class_path
        self.docstring_convention = docstring_convention

    def _yield_sections(self, section_kind: str):
        """
        Obtain sections of a specific kind from the docstring.

        Parameters
        ----------
        section_kind : str
            Kind of section to extract (e.g., "parameters", "returns" in NumPy).
        """
        # Access constructor docstring
        loader = griffe.load(self.class_path)
        func = loader["__init__"]
        if func.docstring:
            sections = func.docstring.parse(self.docstring_convention)
            for section in sections:
                if section.kind == section_kind:
                    for item in section.value:
                        yield item.name, item.description

    @property
    def parameters(self):
        """
        Getter method to extract the parameters from the docstring.

        Returns
        -------
        list
            List of parameters extracted from the docstring.
        """
        return list(self._yield_sections("parameters"))

    @property
    def returns(self):
        """
        Getter method to extract the return values from the docstring.

        Returns
        -------
        list
            List of return values extracted from the docstring.
        """
        return list(self._yield_sections("returns"))
