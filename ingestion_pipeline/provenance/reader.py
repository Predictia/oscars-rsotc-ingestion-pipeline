import functools
import logging
from abc import ABC, abstractmethod

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s — %(name)s — %(levelname)s — %(message)s",
)
logger = logging.getLogger(__name__)


class WorkflowDefinitionReader(ABC):
    """Blueprint class to extract information from the workflow definition."""

    @abstractmethod
    def _load_spec(self):
        """Parse workflow definition file."""
        pass

    @property
    @abstractmethod
    def workflow_inputs(self):
        """Obtain the input parameters from the workflow."""
        pass

    @property
    @abstractmethod
    def steps(self):
        """Obtain the workflow steps."""
        pass


class ArgoDefinitionReader(WorkflowDefinitionReader):
    """
    Information provider for Argo workflows.

    Parameters
    ----------
    workflow_path : str
        Path to the Argo workflow definition.
    """

    def __init__(self, workflow_path: str, ignore_steps: tuple = []):
        self.__workflow_path = workflow_path
        self.__workflow_obj = None
        self.__workflow_entrypoint = None
        self.__steps = None
        self.__steps_raw = None
        self.__tools_raw = None
        self.ignore_steps = list(ignore_steps)

    def _parse_templates(self):
        """Get step and tool data from templates defined in an Argo spec."""
        step_data = {}
        tool_data = {}
        for template in self.__workflow_obj.templates:
            ignore = False
            if template.name in self.ignore_steps:
                logger.debug(
                    f"Template '{template.name}' found in ignore list: not processing template"
                )
                ignore = True
            if template.steps:  # steps
                logger.debug(
                    f"Found {len(template.steps)} substeps under main step {template.name}"
                )
                substep_list = []
                for step in template.steps:
                    _substep_data = list(step.dict().values())
                    try:
                        _substep_data = _substep_data[0][0]
                    except KeyError:
                        raise Exception(
                            "Workflow step data expected to be a list of 1 list"
                        )
                    if ignore or _substep_data["name"] in self.ignore_steps:
                        logger.debug(
                            f"Step <{_substep_data['name']}> within template '{template.name}' "
                            "in ignore list: not processing step"
                        )
                        _substep_data_template = _substep_data.get("template", None)
                        if _substep_data_template:
                            self.ignore_steps.append(_substep_data_template)
                            logger.debug(
                                f"Found template '{_substep_data_template}' under step "
                                f"<{_substep_data['name']}>: adding template to the ignore list"
                            )
                        continue
                    substep_list.append(_substep_data)
                if not ignore:
                    step_data[template.name] = substep_list
            else:
                if not ignore:  # tools
                    tool_data[template.name] = template.dict()
        self.__steps_raw = step_data
        self.__tools_raw = tool_data

    @staticmethod
    def _load_spec(func):
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            if not self.__workflow_obj:
                from hera.workflows import Workflow

                with Workflow.from_file(self.__workflow_path) as w:
                    self.__workflow_obj = w
                    self.__workflow_entrypoint = w.entrypoint
                    self._parse_templates()
            return func(self, *args, **kwargs)

        return wrapper

    def _get_variable_from_step(
        self, variable_type, variable_name, step_data, value_parameter: str = None
    ):
        """Get the input data that matches with the name provided from a given step data.

        Parameters
        ----------
        variable_type : str
            Type of the variable, either 'inputs' or 'outputs'.
        variable_name : str
            Name of the variable within the step's inputs or outpus data.
        step_data : dict
            Data at the step level.
        value_parameter : str, optional
            The parameter to get the variable value from, either 'value' or 'path'.
            If not provided, it will be set to 'value' for inputs and 'path' for outputs by default.
        """
        logger.debug(
            f"Getting variable <{variable_name}> from step data: {step_data}..."
        )
        if not value_parameter:
            if variable_type in ["inputs"]:
                value_parameter = "value"
            elif variable_type in ["outputs"]:
                value_parameter = "path"
        return [
            variable_data[value_parameter]
            for variable_data in step_data
            if variable_name == variable_data["name"]
        ][-1]

    def _resolve_template_variable(self, variable: str, step_data: dict):
        """
        Resolve the value of a template variable in a Argo spec.

        Parameters
        ----------
        variable : str
            Template variable, e.g. "{{workflow.parameters.area}}".
        step_data : dict
            Step data to check for variable resolution at step level.

        Returns
        -------
        str
            The value of the template variable.
        """
        if not variable or not variable.startswith("{{"):
            return variable
        logger.debug(f"Variable <{variable}> is a template variable: parsing it...")
        variable_value = None
        elements = variable.strip("{}").split(".")
        # if len(elements) <= 3:
        #     elements += [None]
        if elements[0] == "workflow":
            logger.debug(
                f"Getting variable <{elements[-1]}> from workflow inputs: "
                f"{self.__workflow_obj.arguments.parameters}..."
            )
            for param in self.__workflow_obj.arguments.parameters:
                if param.name == elements[-1]:
                    logger.debug(
                        f"Found matching parameter <{param.name}> in workflow arguments with "
                        f"value <{param.value}>."
                    )
                    variable_value = param.value
                    break

        elif elements[0] in ["inputs", "outputs"]:
            variable_value = self._get_variable_from_step(
                variable_type=elements[0],
                variable_name=elements[-1],
                step_data=step_data,
                # value_parameter=elements[-1],
            )
        else:
            raise NotImplementedError(
                f"Cannot resolve template variable {variable}: not under the supported scopes:"
                "workflow/inputs/outputs."
            )
        logger.debug(
            f"Resolved template variable <{variable}> to value <{variable_value}>."
        )
        return variable_value

    def _resolve_with_items(self, value, step_data):
        result = value
        if value and value.find("{{item}}") != -1:
            logger.debug(
                f"Value <{value}> corresponds a withItems variable: parsing it..."
            )
            result = [
                value.replace("{{item}}", item) for item in step_data["with_items"]
            ]
            logger.debug(f"Resolved withItems value <{value}> to <{result}>.")
        return result

    def _get_required_param_properties_from(
        self, param_type: str, step_data: list, tool_data: dict = None
    ):
        """Get the required properties from inputs or outputs parameters in Argo spec.

        Parameters
        ----------
        param_type : str
            Type of the parameter, either 'parameters' or 'artifacts'.
        step_data : list
            List of parameters or artifacts data from Argo spec.

        Returns
        -------
        list (of dict)
            List of dicts having only the required input properties.
        """
        if param_type == "parameters":
            param_data = step_data["arguments"]["parameters"]
            required_properties = ["name", "description", "value", "default"]
        elif param_type == "artifacts":
            param_data = step_data["arguments"]["artifacts"]
            required_properties = ["name", "path"]
        elif param_type == "tool_parameters":
            param_data = tool_data["inputs"]["parameters"]
            required_properties = ["name", "description", "value", "default"]
        elif param_type == "tool_artifacts":
            param_data = tool_data["outputs"]["artifacts"]
            required_properties = ["name", "path"]
        else:
            raise ValueError(f"Unknown parameter type: {param_type}")
        param_data_list = []
        logger.debug(
            f"Getting required properties {required_properties} from {param_type} data: {param_data}..."
        )
        for param in param_data:
            required_data = {}
            # Filter only required properties
            param_filtered = {key: param[key] for key in required_properties}
            ## default -> value
            if (
                param_filtered.get("value") is None
                and param_filtered.get("default") is not None
            ):
                param_filtered["value"] = param_filtered["default"]
                param_filtered.pop("default")
            logger.debug(
                f"Resolving {param_type} <{param_filtered['name']}> with raw value {param_filtered}..."
            )
            # Resolve template varibles of type 1) {{item}} and 2) {{workflow.parameters.*}}
            # or {{inputs.parameters.*}} or {{outputs.parameters.*}}
            if param_type in ["parameters", "artifacts"]:
                required_data = dict(
                    [
                        [
                            key,
                            self._resolve_with_items(
                                param_filtered.get(key, None), step_data
                            ),
                        ]
                        for key in required_properties
                    ]
                )
            elif param_type in ["tool_parameters", "tool_artifacts"]:
                required_data = dict(
                    [
                        [
                            key,
                            self._resolve_template_variable(
                                param_filtered.get(key, None), step_data
                            ),
                        ]
                        for key in required_properties
                    ]
                )
            param_data_list.append(required_data)
        return param_data_list

    @property
    @_load_spec
    def data(self):
        """Get data as a dict from the an Argo spec.

        Returns
        -------
        dict
            Dictionary with the Argo spec.
        """
        return self.__workflow_obj.to_dict()

    @_load_spec
    def _normalize_data(self, include_outputs=False):
        steps_found = list(self.__steps_raw.keys())
        logger.debug(f"Steps found in the workflow definition: {steps_found}")
        step_data_list = []
        entrypoint_data = self.__steps_raw[self.workflow_entrypoint]
        for step in entrypoint_data:
            step_name = step["name"]
            logger.debug(
                f"Processing step <{step_name}> from entrypoint <{self.workflow_entrypoint}>..."
            )
            step_template = step["template"]
            logger.debug(
                f"Step <{step_name}> uses template <{step_template}>. Checking whether it has "
                "substeps defined..."
            )
            if step_template in steps_found:
                logger.debug(
                    f"Template <{step_template}> is a (sub)step: processing it..."
                )
                substeps = self.__steps_raw[step_template]
                logger.debug(f"Step <{step_name}> has {len(substeps)} substeps.")
                if len(substeps) > 1:
                    raise NotImplementedError(
                        "Currently not implemented the case of a step with multiple substeps: "
                        f"step <{step_name}> has {len(substeps)} substeps."
                    )
                substep = substeps[0]
                logger.debug(
                    f"Processing substep <{substep['name']}> of step <{step_name}>..."
                )
                ## Inputs (and outputs if requested)
                substep_inputs = self._get_required_param_properties_from(
                    "parameters", substep
                )
                if include_outputs:
                    substep_outputs = self._get_required_param_properties_from(
                        "artifacts", substep
                    )
                ## Tools
                substep_template = substep.get("template", None)
                if substep_template in steps_found:
                    raise NotImplementedError(
                        "Currently not implemented the case of a substep with a template that "
                        f"is also a (sub)step: substep <{substep['name']}> of step <{step_name}> "
                        f"uses template <{substep_template}> that is also a (sub)step."
                    )
                tool_name = (
                    substep_template if substep_template in self.__tools_raw else None
                )
                if not tool_name:
                    raise ValueError(
                        f"Could not find a tool for substep <{substep['name']}> of step <{step_name}>: "
                        f"with template name <{substep_template}>."
                    )
                logger.debug(
                    f"Substep <{substep['name']}> uses template <{tool_name}> as tool"
                )
                logger.debug(f"Processing tool <{tool_name}>...")
                if tool_name in self.__tools_raw:
                    tool_inputs = self._get_required_param_properties_from(
                        "tool_parameters",
                        step_data=substep_inputs,
                        tool_data=self.__tools_raw[tool_name],
                    )
                    if include_outputs:
                        tool_outputs = self._get_required_param_properties_from(
                            "tool_artifacts",
                            step_data=substep_outputs,
                            tool_data=self.__tools_raw[tool_name],
                        )
                step_data_list.append(
                    {
                        step_name: {
                            "inputs": substep_inputs,
                            "outputs": substep_outputs if include_outputs else None,
                            "tools": {
                                tool_name: {
                                    "inputs": tool_inputs,
                                    "outputs": tool_outputs
                                    if include_outputs
                                    else None,
                                }
                            },
                        }
                    }
                )
            else:
                raise NotImplementedError(
                    "Currently not implemented the case of a step with a template that is not defined "
                    f"as a (sub)step: step <{step_name}> uses template <{step_template}> that is not "
                    "defined as a (sub)step."
                )
        return {self.workflow_entrypoint: {"entrypoint": True, "steps": step_data_list}}

    @property
    def steps(self):
        """Get the steps from the (main) template in an Argo spec.

        Returns
        -------
        dict
            Step data including inputs and tool when requested.
        """
        if not self.__steps:
            self.__steps = self._normalize_data()
        return self.__steps

    @property
    def workflow_entrypoint(self):
        """Get the main identifier of the workflow through 'entrypoint' in Argo spec.

        Returns
        -------
        str
            Argo spec's entrypoint name.
        """
        return self.__workflow_entrypoint

    @property
    @_load_spec
    def workflow_inputs(self):
        """Get workflow's input parameters.

        Returns
        -------
        generator
            Iterator with the workflow input arguments.
        """
        try:
            for param in self.__workflow_obj.arguments.parameters:
                yield {
                    "name": param.name,
                    "value": param.value,
                    "description": param.description,
                }
        except AttributeError:
            logger.warning(
                "Argo workflow specification has no input parameters declared"
            )

    @_load_spec
    def get_step(self, step_name: str):
        """Get the step data that matches with the name provided.

        Returns
        -------
        dict
            Step data.
        """
        for step_data in self.steps[self.workflow_entrypoint]["steps"]:
            if step_name in step_data.keys():
                return step_data[step_name]

    def get_tool(self, tool_name, step_name):
        step_data = self.get_step(step_name)
        return step_data["tools"][tool_name]

    def get_tool_input(self, input_name, tool_name, step_name):
        return [
            input_data
            for input_data in self.get_tool(tool_name, step_name)["inputs"]
            if input_name == input_data["name"]
        ][-1]
