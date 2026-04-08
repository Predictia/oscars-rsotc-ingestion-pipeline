import functools
import logging
from pathlib import Path
from types import MappingProxyType

from rocrate.model import ComputerLanguage, ContextEntity
from rocrate.model.person import Person
from rocrate.rocrate import ROCrate

logger = logging.getLogger(__name__)

PROFILE_SETTINGS = MappingProxyType(
    {
        "id": "https://w3id.org/ro/wfrun/{type}/{version}",
        "name": "{type} Run Crate",
    }
)


def _set_default_identifier(
    builder,
    entity_type: str,
    properties: dict,
):
    """Set a default identifier (@id) for an entity.

    Parameters
    ----------
    builder : WorkflowBuilder
        WorkflowBuilder object containing the RO-Crate definition.
    entity_type : str
        Name of the entity.
    properties : dict
        Dictionary with the properties of the entity.
    """
    if properties is None:
        raise ValueError("Properties dictionary cannot be empty.")

    if "@id" in properties or entity_type in ["workflow", "workflow_engine", "action"]:
        return
    elif "identifier" in properties:
        entity_name = properties.pop("identifier")
    else:
        entity_name = properties.get("name", "unknown-variable")

    prepend_workflow_name = False
    prepend_hash_sign = True
    has_parent = False
    workflow_prefix = builder.workflow_name
    parent_prefix = ""
    if entity_type == "lang":
        prepend_workflow_name = True
        workflow_prefix = "https://w3id.org/workflowhub/workflow-ro-crate"
    elif entity_type in ["input_param", "step"]:
        prepend_workflow_name = True
        has_parent = True
    elif entity_type == "tool":
        prepend_workflow_name = True
    elif entity_type == "input_value":
        has_parent = True
        parent_prefix = "pv-"
    elif entity_type == "output_value":
        prepend_hash_sign = False

    identifier = entity_name
    if has_parent:
        parent = properties.pop("parent", "")
        if not parent:
            raise Exception(
                f"Entity {entity_name} is expected to have a parent step/tool through 'parent' property"
            )
        identifier = f"{parent_prefix}{parent}/{entity_name}"
    if prepend_workflow_name:
        identifier = f"{workflow_prefix}#{identifier}"
    else:
        if prepend_hash_sign:
            identifier = f"#{identifier}"
    properties.update({"@id": identifier})
    logger.debug(f"Setting default @id for {entity_name}: {identifier}")


def _set_default_identifier_for_entity(entity_type: str):
    """Create a decorator factory to set a default identifier (@id) for the given entity type."""

    def decorator(func: callable):
        @functools.wraps(func)
        def wrapper(self, properties: dict = None, **kwargs):
            if properties is None:
                properties = {}
            _set_default_identifier(self, entity_type, properties, **kwargs)
            return func(self, properties=properties, **kwargs)

        return wrapper

    return decorator


def _validate_properties(entity_type: str):
    """Create a decorator that ensures compliance of property attributes according to the entity type."""

    def decorator(func: callable):
        @functools.wraps(func)
        def wrapper(self, properties: dict = None, **kwargs):
            validated_properties = {}
            if properties is None:
                return validated_properties
            logger.debug(f"Validating properties for {entity_type}: {properties}")
            allowed_properties = []
            if entity_type == "input_param":
                # add 'additionalType'
                if not properties.get("additionalType", None):
                    input_value = properties.get("value", None)
                    input_type = "Text"
                    if not input_value:
                        logger.warning(
                            f"Cannot get 'value' for input parameter with properties <{properties}>: "
                            "assuming 'Text'"
                        )
                    else:
                        try:
                            if Path(input_value).is_file():
                                input_type = "File"
                        except TypeError:
                            pass
                    properties["additionalType"] = input_type
                    logger.debug(
                        f"Added additionalType for input parameter with properties {properties}"
                    )
                # validate
                allowed_properties = [
                    "@id",
                    "@type",
                    "exampleOfWork",
                    "name",
                    "description",
                    "additionalType",
                ]
            validated_properties = {
                key: properties[key] for key in allowed_properties if key in properties
            }
            logger.debug(
                f"Resultant validation for properties of type {entity_type}: {validated_properties}"
            )

            return func(self, properties=validated_properties, **kwargs)

        return wrapper

    return decorator


class WorkflowBuilder:
    """
    Helper class to build a workflow RO-Crate with workflow entities.

    Parameters
    ----------
    name : str
        Name of the workflow.
    path : str
        Path to the workflow script.
    rocrate_profile : str
        Formal RO-Crate profile identifier to be used to generate the provenance.
    rocrate_gen_preview : bool
        Flag to generate the crate in HTML format.
    authors : list
        Authors of the crate.
    """

    def __init__(
        self,
        name: str,
        path: str,
        rocrate_profile: str = None,
        rocrate_gen_preview: bool = False,
        authors: list = [],
        orgs: list = [],
    ):
        self.workflow_name = name
        self.__workflow_path = path
        self.__rocrate_profile = rocrate_profile
        self.__rocrate_gen_preview = rocrate_gen_preview
        self.__authors = authors
        self.__provenance_run_crate = False
        self.__workflow = None
        self.__workflow_engine = None
        self.__lang = None
        self.__steps = []
        self.__tools = []
        self.__action = None
        self.__actions = []
        self.__inputs = {"parameters": [], "values": []}
        self.__input_value = None
        self.__outputs = []
        self.__output_value = None
        # Initialize RO-Crate
        self.crate = self._init_crate()
        self._add_profiles()
        if self.__authors:
            self._add_authors()

    def _init_crate(self):
        """
        Init crate object according to static and runtime settings.

        Returns
        -------
        ROCrate
            RO-Crate entity object
        """
        # Instantiate RO-Crate with initial definition
        crate = ROCrate(gen_preview=self.__rocrate_gen_preview)

        crate.name = "RO-Crate provenance for Regional State of the Climate (RSOTC) Data and Indices"
        crate.description = (
            "A crate containing the provenance of a RSOTC ingestion pipeline run"
        )
        crate.root_dataset["conformsTo"] = [
            {"@id": profile} for profile in crate.metadata["conformsTo"]
        ]
        return crate

    def _add_contextual_entity(self, type: str, properties: dict) -> ContextEntity:
        """
        Add an generic entity through ContextEntity to the RO-Crate.

        Parameters
        ----------
        type : str
            RO-Crate entity type.
        properties : dict
            Dictionary containing the main features of the entity.

        Returns
        -------
        ContextEntity
            RO-Crate contextual entity object of the given type.
        """
        identifier = properties.pop("@id", "")
        id_dict = {"identifier": identifier}
        if type in ["PropertyValue"] and "value" not in properties:
            raise ValueError("Output properties must include a 'value' key.")
        elif type in ["Dataset", "File"]:
            dest_path = properties.pop("dest_path")
            source_path = properties.pop("source_path")
            logger.debug(
                f"Storing <{identifier}> file under {dest_path} path within the crate (source: {source_path})"
            )
            # Use appropriate method according to entity type (defaults to 'add_file')
            add_method = self.crate.add_file
            if type == "Dataset":
                add_method = self.crate.add_dataset
            try:
                entity = add_method(
                    source=source_path,
                    dest_path=dest_path,
                    properties=properties,
                )
            except FileNotFoundError as e:
                raise Exception(f"Cannot create entity of type '{type}': {str(e)}")
        else:
            entity = self.crate.add(
                ContextEntity(
                    self.crate,
                    **id_dict,
                    properties={
                        "@type": type,
                        **properties,
                    },
                )
            )
        return entity

    def _add_profiles(self):
        """Add RO-Crate profiles according to the given settings."""
        _profile_items = self.__rocrate_profile.split("-")
        if _profile_items[0] not in ["process", "workflow", "provenance"]:
            raise ValueError(f"RO-Crate profile not supported: {self.rocrate_profile}")
        _types = ["process", "workflow"]
        if _profile_items[0] == "provenance":
            _types.append("provenance")
            self.__provenance_run_crate = True
        _version = _profile_items[-1]
        profiles = []
        for _type in _types:
            profiles.append(
                self._add_contextual_entity(
                    "CreativeWork",
                    properties={
                        "@id": PROFILE_SETTINGS["id"].format(
                            type=_type, version=_version
                        ),
                        "name": PROFILE_SETTINGS["name"].format(
                            type=_type.capitalize()
                        ),
                        "version": _version,
                    },
                )
            )
            self.crate.root_dataset["conformsTo"] = profiles

    def _add_authors(self):
        """Add authors to the RO-Crate."""
        author_ids = []
        for author in self.__authors:
            orcid = author.pop("orcid", None)
            if not orcid:
                raise Exception(
                    f"Author's ORCID information missing for author: {author}"
                )
            self.crate.add(Person(self.crate, orcid, properties=author))
            author_ids.append({"@id": orcid})
        self.crate.root_dataset["author"] = author_ids

    def decompose_id(self, entity_id: str, entity_type: str):
        if entity_type in ["tool"]:
            workflow_id, tool_id = entity_id.split("#")
            result = (workflow_id, tool_id)
        elif entity_type in ["input_param"]:
            workflow_id, rest = entity_id.split("#")
            tool_id, input_id = rest.split("/")
            result = (workflow_id, tool_id, input_id)
        return result

    @property
    def workflow_path(self):
        """
        Getter method that returns the path to the workflow file.

        Returns
        -------
        str
            Path to the workflow file
        """
        return self.__workflow_path

    @property
    def workflow(self):
        """
        Getter method that returns the workflow definition.

        Returns
        -------
        rocrate.model.computationalworkflow.ComputationalWorkflow
            Instance of RO-Crate workflow
        """
        return self.__workflow

    @workflow.setter
    @_set_default_identifier_for_entity(entity_type="workflow")
    def workflow(self, properties: dict = None):
        """
        Setter method that configures the (non-CWL) workflow script.

        Parameters
        ----------
        properties : dict
            Dictionary containing the main features to characterize the workflow.
        """
        if self.__provenance_run_crate:
            properties["@type"] = [
                "File",
                "SoftwareSourceCode",
                "ComputationalWorkflow",
                "HowTo",
            ]

        self.__workflow = self.crate.add_workflow(
            source=self.__workflow_path,
            dest_path=self.__workflow_path,
            main=True,
            lang=self.__lang,
            gen_cwl=False,
            properties=properties,
        )

    @property
    def workflow_run(self):
        return self.__workflow_run

    @workflow_run.setter
    def workflow_run(self, identifier):
        self.__workflow_run = identifier

    @property
    def workflow_engine(self):
        """
        Getter method that returns the workflow workflow engine (software).

        Returns
        -------
        rocrate.model.contextentity.ContextEntity
            An instance of SoftwareApplication's contextual entity
        """
        return self.__workflow_engine

    @workflow_engine.setter
    @_set_default_identifier_for_entity(entity_type="workflow_engine")
    def workflow_engine(self, properties: dict = None):
        """
        Setter method that defines the workflow engine (software).

        Parameters
        ----------
        properties : dict
            Dictionary containing the definition of the workflow engine (software).
        """
        self.__workflow_engine = self._add_contextual_entity(
            "SoftwareApplication", properties
        )

    @property
    def lang(self):
        """
        Getter method that returns the programming language definition.

        Returns
        -------
        rocrate.model.computerlanguage.ComputerLanguage
            RO-Crate programming language object.
        """
        return self.__lang

    @lang.setter
    @_set_default_identifier_for_entity(entity_type="lang")
    def lang(self, properties: dict = None):
        """
        Setter method that defines the programming language of the workflow script.

        Parameters
        ----------
        properties : dict
            Dictionary containing the main features to characterize the programming language.
        """
        identifier = properties.pop("@id")
        self.__lang = ComputerLanguage(
            self.crate, identifier=identifier, properties=properties
        )
        self.crate.add(self.__lang)

    @property
    def steps(self):
        """
        Getter method that returns the workflow steps.

        Returns
        -------
        list
            list of workflow steps
        """
        return self.__steps

    @property
    def step(self):
        """
        Getter method that returns the definition of a step in the workflow.

        Returns
        -------
        rocrate.model.contextentity.ContextEntity
            RO-Crate step object.
        """
        return self.__steps[-1] if self.__steps else None

    @step.setter
    @_set_default_identifier_for_entity(entity_type="step")
    def step(self, properties: dict = None):
        """
        Setter method that defines a step in the workflow.

        Parameters
        ----------
        properties : dict
            Dictionary containing the definition of the workflow steps.
        """
        step_entity = self._add_contextual_entity("HowToStep", properties)
        self.__steps.append(step_entity)

    @property
    def tools(self):
        """
        Getter method that returns the workflow tools.

        Returns
        -------
        list
            list of workflow tools
        """
        return self.__tools

    @property
    def tool(self):
        """
        Getter method that returns the definition of a tool in the workflow.

        Returns
        -------
        rocrate.model.contextentity.ContextEntity
            RO-Crate tool object.
        """
        return self.__tools[-1] if self.__tools else None

    @tool.setter
    @_set_default_identifier_for_entity(entity_type="tool")
    def tool(self, properties: dict = None):
        """
        Setter method that defines a tool in the workflow.

        Parameters
        ----------
        properties : dict
            Dictionary containing the definition of the workflow tools.
        """
        tool_inputs = properties.pop("inputs")
        tool_entity = self._add_contextual_entity("SoftwareApplication", properties)
        # tool inputs
        for tool_input in tool_inputs:
            self.input = tool_input
            tool_entity.append_to("input", {"@id": self.input["@id"]})
        self.__tools.append(tool_entity)

    @property
    def actions(self):
        """
        Getter method that returns the actions performed by the workflow.

        Returns
        -------
        list
            list of RO-Crate actions
        """
        return self.__actions

    @property
    def action(self):
        """
        Getter method that provides an action's main features.

        Returns
        -------
        rocrate.model.contextentity.ContextEntity
            RO-Crate action object for the action (CreateAction entity).
        """
        return self.__action

    @action.setter
    @_set_default_identifier_for_entity(entity_type="action")
    def action(self, properties: dict = None):
        """
        Setter method that sets the characteristics of an action performed by the workflow.

        Parameters
        ----------
        properties : dict
            Dictionary containing the workflow action's characteristics.
        """
        action_type = properties.get("@type", "CreateAction")
        self.__action = self._add_contextual_entity(action_type, properties=properties)
        self.__actions.append(self.__action)

    @property
    def inputs(self):
        """
        Getter method that returns the workflow input list.

        Returns
        -------
        list
            list of RO-Crate workflow inputs
        """
        return self.__inputs

    @property
    def input(self):
        """
        Getter method that provides the definition of an input of the workflow.

        Returns
        -------
        rocrate.model.contextentity.ContextEntity
            RO-Crate input parameter object for the input (FormalParameter entity).
        """
        return self.__inputs["parameters"][-1] if self.__inputs else None

    @input.setter
    @_set_default_identifier_for_entity(entity_type="input_param")
    @_validate_properties(entity_type="input_param")
    def input(self, properties: dict = None):
        """
        Setter method that defines an input of the workflow.

        Parameters
        ----------
        properties : dict
            Dictionary containing the definition of the input parameter.
        """
        input_entity = self._add_contextual_entity(
            "FormalParameter", properties=properties
        )
        self.__inputs["parameters"].append(input_entity)

    @property
    def input_value(self):
        """
        Getter method that provides the definition of the value of an input parameter of the workflow.

        Returns
        -------
        rocrate.model.contextentity.ContextEntity
            RO-Crate input value object (PropertyValue entity).
        """
        return self.__inputs["values"][-1] if self.__inputs else None

    @input_value.setter
    @_set_default_identifier_for_entity(entity_type="input_value")
    def input_value(self, properties: dict = None):
        """
        Setter method that defines a value of an input parameter of the workflow.

        Parameters
        ----------
        properties : dict
            Dictionary containing the definition of the input value.
        """
        if "value" not in properties:
            raise ValueError("Input properties must include a 'value' key.")
        self.__input_value = self._add_contextual_entity(
            "PropertyValue", properties=properties
        )
        self.__inputs["values"].append(self.__input_value)

    def get_input_value(self, input_value_id: str, from_input_param_id: bool = True):
        """
        Get input value that matches the given identifier.

        Parameters
        ----------
        input_value_id: str
            Identifier of the input value to be retrieved.
        """
        if from_input_param_id:
            input_value_list = [
                input_value
                for input_value in self.inputs["values"]
                if input_value.get("exampleOfWork").id == input_value_id
            ]
        else:
            input_value_list = [
                input_value
                for input_value in self.inputs["values"]
                if input_value.id == input_value_id
            ]
        return input_value_list[-1]

    def get_input_id(self, input_id: str, tool_id: str):
        """
        Get input parameter data from the given tool.

        Parameters
        ----------
        tool : str
            Identifier of the tool.
        """
        logger.debug(f"Getting {input_id} input from {tool_id} tool")
        for input_param in self.inputs["parameters"]:
            _, _tool, _input = self.decompose_id(
                input_param.id, entity_type="input_param"
            )
            if _tool == tool_id and _input == input_id:
                logger.debug(
                    f"Found matching input for (input: '{input_id}', tool: '{tool_id}'): "
                    f"{input_param.id}"
                )
                return input_param.id

    @property
    def outputs(self):
        """
        Getter method that returns the workflow output list.

        Returns
        -------
        list
            list of RO-Crate workflow outputs
        """
        return self.__outputs

    @property
    def output(self):
        """
        Getter method that provides the definition of an output of the workflow.

        Returns
        -------
        rocrate.model.contextentity.ContextEntity
            RO-Crate output parameter object (FormalParameter entity).
        """
        return self.__outputs[-1] if self.__outputs else None

    @output.setter
    @_set_default_identifier_for_entity(entity_type="output_param")
    def output(self, properties: dict = None):
        """
        Setter method that defines an output parameter of the workflow.

        Parameters
        ----------
        properties : dict
            Dictionary containing the definition of the output parameter.
        """
        # Validate "properties" has the required keys
        output_entity = self._add_contextual_entity(
            "FormalParameter", properties=properties
        )
        self.__outputs.append(output_entity)

    @property
    def output_value(self):
        """
        Getter method that provides the definition of the value of an output parameter of the workflow.

        Returns
        -------
        rocrate.model.contextentity.ContextEntity
            RO-Crate output value object (PropertyValue entity).
        """
        return self.__output_value

    @output_value.setter
    @_set_default_identifier_for_entity(entity_type="output_value")
    def output_value(self, properties: dict = None):
        """
        Setter method that defines a value of an output parameter of the workflow.

        Parameters
        ----------
        properties : dict
            Dictionary containing the definition of the output value.
        """
        _type = properties.get("@type", "")
        self.__output_value = self._add_contextual_entity(
            type=_type, properties=properties
        )

    def add_file(self, properties: dict):
        """
        Setter method that adds an individual file to the workflow.

        Parameters
        ----------
        properties : dict
            Dictionary containing the definition of the file to add. At the very
            least the properties dictionary MUST contain the '@id' property.
        """
        self._add_contextual_entity("File", properties=properties)

    def add_entity_size(self, properties: dict):
        """
        Setter method for adding the size of a File or Dataset using schema.org'2 QuantitativeValue.

        Parameters
        ----------
        properties : dict
            Dictionary containing the definition of the file to add. At the very
            least the properties dictionary MUST contain the '@id' property.
        """
        self._add_contextual_entity("QuantitativeValue", properties=properties)
