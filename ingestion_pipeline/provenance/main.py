import copy
import logging
import os.path
from pathlib import Path
from types import MappingProxyType
from typing import Union

import yaml

from ingestion_pipeline.provenance.builder import WorkflowBuilder
from ingestion_pipeline.provenance.reader import ArgoDefinitionReader

logger = logging.getLogger(__name__)

MIME_TYPES_PER_EXTENSION = MappingProxyType(
    {
        ".zarr": [
            "application/zarr",
            {"@id": "https://zarr-specs.readthedocs.io/en/latest/specs.html"},
        ],
    }
)


class ProvenanceTrackerStaticInfo:
    def __init__(self, config_file: str):
        # Class attributes
        self.config_file = config_file
        self.config = None
        # Load static configuration
        try:
            with open(self.config_file, "r") as f:
                self.config = yaml.safe_load(f)
        except FileNotFoundError:
            logger.error(f"Configuration file not found: {self.config_file}")
            raise
        except yaml.YAMLError as e:
            logger.error(f"Error parsing YAML file: {self.config_file} - {e}")
            raise

    def _validate_outputs(self, check_path_presence: bool = False):
        """Validate that required artifacts are present in the configuration."""
        required_keys = ["workflow"]

        # Check top-level structure
        for key in required_keys:
            assert key in self.config, f"Missing required key '{key}' in configuration"

        # Check workflow structure
        assert (
            "runs" in self.config["workflow"]
        ), "Missing 'runs' in workflow configuration"

        # Validate each run has required artifact information
        for i, run in enumerate(self.config["workflow"]["runs"]):
            # Validate artifacts have required fields
            for j, artifact in enumerate(run.get("artifacts", [])):
                assert (
                    "name" in artifact
                ), f"Run '{run.get('name', i)}', artifact {j} missing 'name' field"
                assert (
                    "dest_path" in artifact
                ), f"Run '{run.get('name', i)}', artifact {j} missing 'dest_path' field"
                logger.debug(
                    f"Run '{run.get('name', i)}', artifact {j} 'dest_path' field is "
                    f"{artifact.keys()}: {artifact['dest_path']}"
                )
                assert not Path(artifact["dest_path"]).is_absolute(), (
                    f"Run '{run.get('name', i)}', artifact {j} dest_path must be relative: "
                    f"{artifact['dest_path']}"
                )
                assert Path(artifact["source_path"]).is_absolute(), (
                    f"Run '{run.get('name', i)}', artifact {j} source_path must be absolute: "
                    f"{artifact['source_path']}"
                )
                # Check file presence only if requested
                if check_path_presence:
                    assert Path(artifact["source_path"]).exists(), (
                        f"Run '{run.get('name', i)}', artifact {j} source_path does "
                        f"not exist: {artifact['source_path']}"
                    )

        logger.debug("Configuration validation passed")

    def validate(self, **kwargs):
        self._validate_outputs(**kwargs)

    @property
    def output_paths(self):
        path_data = {}
        for run in self.config["workflow"]["runs"]:
            for artifact in run["artifacts"]:
                path_data[artifact["name"]] = {
                    "source_path": artifact.get("source_path", None),
                    "dest_path": artifact.get("dest_path", None),
                }
        return path_data

    @output_paths.setter
    def output_paths(self, path_data):
        """
        Set output paths of a collection of artifacts.

        Parameters
        ----------
        path_data : dict
            Dictionary containing the name and path of the dataset
        """
        # Traverse each run and its artifacts
        for run in self.config["workflow"]["runs"]:
            if "artifacts" not in run:
                logger.warning(f"No 'artifacts' section found for workflow run {run}")
                continue
            for artifact in run["artifacts"]:
                if artifact["name"] not in path_data:
                    logger.warning(
                        f"Artifact {artifact['name']} not found in path data"
                    )
                    continue
                artifact_data_to_update = path_data[artifact["name"]]
                data_to_update = dict(
                    [
                        [key, artifact_data_to_update[key]]
                        for key in ["source_path", "dest_path"]
                        if key in artifact_data_to_update
                        and artifact_data_to_update[key] is not None
                    ]
                )
                artifact.update(data_to_update)
                logger.debug(
                    f"Updated path in static metadata file for dataset '{artifact['name']}': "
                    f"{data_to_update}"
                )

    @property
    def authors(self):
        return self.config["workflow"]["authors"]

    @property
    def orgs(self):
        return self.config["workflow"]["organizations"]

    @property
    def license(self):
        return self.config["workflow"].get("license", None)

    @property
    def workflow_runs(self):
        return self.config["workflow"]["runs"]

    def get_output(self, input_name: str, input_value: str):
        for run in self.config["workflow"]["runs"]:
            for input in run["inputs"]:
                if input["name"] == input_name and input["value"] == input_value:
                    logger.debug(
                        f"Found output artifacts for the given input (name: {input_name}, "
                        f"value: {input_value}): {run['artifacts']}"
                    )
                    return run["artifacts"]


class ProvenanceMetadataUtils:
    @staticmethod
    def get_media_format(file_name):
        from mimetypes import guess_type

        mime_type, encoding = guess_type(file_name)
        if mime_type:
            return mime_type
        else:
            return MIME_TYPES_PER_EXTENSION[Path(file_name).suffix]

    @staticmethod
    def get_license():
        # from pyproject.toml
        if Path("pyproject.toml").exists():
            try:
                with open("pyproject.toml", "rb") as f:
                    import tomllib

                    data = tomllib.load(f)
                project_metadata = data.get("project", {})
                license_info = project_metadata.get("license")
                if isinstance(license_info, str):
                    return license_info
                elif isinstance(license_info, dict):
                    if license_info.get("text", None):
                        return license_info["text"]
                logger.warning(
                    "Cannot get license information from pyproject.toml: 'license' property has value: "
                    f"{license_info}"
                )
            except FileNotFoundError:
                logger.debug("Cannot get license from pyproject.toml: file not found.")
            except tomllib.TOMLDecodeError:
                logger.debug("Cannot get license from pyproject.toml: wrong format.")
        else:
            return "Unknown"


def prospective_provenance(workflow_builder, static_metadata, reader):
    # From `argo version`
    workflow_builder.lang = {
        "identifier": "argo",
        "name": "Argo Workflows",
        "url": "https://argoproj.github.io/workflows/",
        "version": "v4.0.0",
    }
    workflow_builder.workflow = {
        "name": "Ingestion Pipeline workflow",
        "description": (
            "Ingestion pipeline workflow that downloads and computes derived indices out of ERA5 dataset."
        ),
        "encodingFormat": "application/yaml",
        "contentSize": os.path.getsize(workflow_builder.workflow_path),
    }
    workflow_crate = workflow_builder.workflow
    logger.debug(f"Added workflow entity to the crate: {workflow_crate}")

    workflow_builder.workflow_engine = {"name": "Argo Workflows v4.0.0"}
    logger.debug(
        f"Added workflow engine entity to the crate: {workflow_builder.workflow_engine}"
    )

    # Workflow input parameters
    for input_data in reader.workflow_inputs:
        workflow_builder.input = {
            "parent": reader.workflow_entrypoint,
            "name": input_data["name"],
            "value": input_data["value"],  # needed to extract 'additionalType'
            "description": input_data.get("description", None),
        }
        workflow_crate.append_to("input", {"@id": workflow_builder.input["@id"]})
    logger.debug(
        f"Input parameters added to the crate workflow: {workflow_builder.inputs}"
    )

    # # Workflow output parameters
    # if config:
    #     for output_param in config["workflow"]["outputs"]["parameters"]:
    #         workflow_builder.input = {
    #             "parent": output_param["output_of"],
    #             "name": output_param["name"],
    #             # "description": description,
    #         }
    #         workflow_crate.append_to("input", {"@id": workflow_builder.input["@id"]})
    #     logger.debug(f"Input parameters added to the crate workflow: {workflow_builder.inputs}")

    # Workflow steps
    steps_copy = copy.deepcopy(reader.steps)
    for step_name, step_data in steps_copy.items():
        if step_data["entrypoint"]:
            workflow_builder.workflow_run = step_name
        for substep in step_data["steps"]:
            # step
            substep_name, substep_data = substep.popitem()
            workflow_builder.step = {"parent": step_name, "name": substep_name}
            # step tools
            for tool_name, tool_data in substep_data["tools"].items():
                tool_inputs = [
                    {**tool_input, "parent": tool_name}
                    for tool_input in tool_data["inputs"]
                ]
                workflow_builder.tool = {
                    "name": tool_name,
                    "inputs": tool_inputs,
                }
                workflow_builder.step.append_to(
                    "workExample", {"@id": workflow_builder.tool["@id"]}
                )
                workflow_crate.append_to("step", {"@id": workflow_builder.step["@id"]})
                workflow_crate.append_to(
                    "hasPart", {"@id": workflow_builder.tool["@id"]}
                )
    logger.debug(f"Steps added to the crate workflow: {workflow_builder.steps}")


def retrospective_provenance(workflow_builder, static_metadata, reader):
    def _gather_tool_input_ids(tool_inputs):
        """
        Gather the names in WorkflowEngine format of the required inputs for the tool.

        Returns
        -------
        list
            A list of WorkflowEngine input parameters that are required for the tool.
        """
        for tool_input in tool_inputs:
            # Get input value entity from the identifier of the input parameter
            input_param_entity = workflow_builder.crate.dereference(tool_input.id)
            input_param_name = input_param_entity.properties().get("name", None)
            if not input_param_name:  # get it from "@id"
                _, __, input_name = workflow_builder.decompose_id(
                    input_param_entity.id, entity_type="input_param"
                )
            yield input_param_name

    def validate_completeness(input_parameters, entity_type):
        if entity_type == "input_param":
            required_inputs = _gather_tool_input_ids(tool_inputs=step_tool_inputs)
            logger.debug(
                f"Checking if required inputs <{required_inputs}> (from WorkflowBuilder) are present "
                "in WorkflowEngine inputs"
            )
            return [
                required_input
                for required_input in required_inputs
                if required_input not in input_parameters.keys()
                or not input_parameters[required_input]
            ]

    ## Get workflow execution object
    from ingestion_pipeline.provenance.engine import ArgoWorkflowEngine

    workflow_engine = ArgoWorkflowEngine()
    workflow_data = workflow_engine.get_main_workflow_data()

    # 1. CreateAction (1-to-1: Workflow)
    workflow_builder.action = {
        "@type": "CreateAction",
        "name": f"Run of workflow/{workflow_builder.workflow.id}#{reader.workflow_entrypoint}",
        "instrument": {"@id": workflow_builder.workflow.id},
        # TO BE ADDED LATER: "object" (ALL constant values), "result" (ALL output values)
    }
    create_action_main = workflow_builder.action
    # Add main CreateAction to 'mentions' in the Root Dataset
    workflow_builder.crate.root_dataset.append_to(
        "mentions", {"@id": create_action_main.id}
    )

    # 2. OrganizeAction (1-to-1: Workflow engine)
    workflow_builder.action = {
        "@type": "OrganizeAction",
        "name": f"Run of {workflow_builder.workflow_engine.get('name')}",
        "agent": "",  # TBD
        "instrument": {"@id": workflow_builder.workflow_engine.id},
        "result": {"@id": create_action_main.id},
        "startTime": workflow_data.dict().get("started_at").isoformat() or None,
        "endTime": workflow_data.dict().get("finished_at").isoformat() or None,
        # TO BE ADDED LATER: "object" (control_action)
    }
    organize_action = workflow_builder.action

    # 3. ControlAction (1-to-1: Workflow step)
    for step in workflow_builder.steps:
        step_name = step.get("name")
        logger.debug(f"Gathering retrospective provenance: step ({step_name})")
        workflow_builder.action = {
            "@type": "ControlAction",
            "instrument": {"@id": step.id},
            "name": f"Orchestrate {step_name}",
            # TO BE ADDED LATER: "object" (create_action)
        }
        control_action = workflow_builder.action
        organize_action.append_to("object", {"@id": control_action.id})

        # 4. CreateAction (tool execution): many-to-1 with ControlAction, 1-to-many with PropertyValue/File
        ## Get associated tool (SoftwareApplication, 1-to-1 with HowToStep)
        step_tool = step.get("workExample")[
            -1
        ]  # only one workExample entity ID expected
        logger.debug(
            f"Gathering retrospective provenance: step ({step_name}) > tool ({step_tool.id})"
        )
        step_tool_entity = workflow_builder.crate.dereference(step_tool.id)
        ## Gather input params (FormalParameter, many-to-1 with SoftwareApplication)
        step_tool_inputs = step_tool_entity.get("input")
        logger.debug(
            f"Input parameters found in WorkflowBuilder for {step_tool.id} tool: {step_tool_inputs}"
        )

        ## Gather execution data for tool
        _, tool_name = workflow_builder.decompose_id(
            entity_id=step_tool.id, entity_type="tool"
        )
        execution_data = workflow_engine.get_execution_data(tool_name=tool_name)
        if not execution_data:
            raise Exception(
                f"No execution data could be obtained from engine's API for {tool_name} tool"
            )
        for _execution_id, _data in execution_data.items():
            logger.debug(
                f"Gathering inputs and outputs for tool execution (CreateAction) with id: {_execution_id}"
            )
            _input_parameters_from_engine = workflow_engine.get_execution_parameters(
                _data
            )
            _invalid_input_parameters = validate_completeness(
                _input_parameters_from_engine, entity_type="input_param"
            )
            if _invalid_input_parameters:
                raise Exception(
                    f"Found incomplete input arguments for '{tool_name}' tool with execution identifier "
                    f"'{_execution_id}': {_invalid_input_parameters}"
                )
            else:
                logger.debug(
                    "All the required parameters are present in WorkflowBuilder inputs"
                )
            logger.debug(
                f"Input parameters for '{tool_name}' tool with execution identifier '{_execution_id}': "
                f"{_input_parameters_from_engine}"
            )

            step_run_no = 1
            workflow_builder.action = {
                "identifier": _execution_id,
                "@type": "CreateAction",
                "name": f"Run of {step.id}_{step_run_no}",
                "instrument": {"@id": step_tool.id},
                "startTime": _data.dict().get("started_at").isoformat() or "",
                "endTime": _data.dict().get("finished_at").isoformat() or "",
                # TO BE ADDED LATER: "result" (output values)
            }
            # Add CreateAction to ControlAction
            control_action.append_to("object", {"@id": workflow_builder.action.id})

            # Create input values
            for _input_param, _input_value in _input_parameters_from_engine.items():
                logger.debug(f"Getting '{_input_param}' input from '{tool_name}' tool")
                workflow_builder_input_id = workflow_builder.get_input_id(
                    _input_param, tool_name
                )
                workflow_builder.input_value = {
                    "parent": tool_name,
                    "name": f"{_input_param}_{step_run_no}",
                    "value": _input_value,
                    "exampleOfWork": {"@id": workflow_builder_input_id},
                }
                workflow_builder.action.append_to(
                    "object", {"@id": workflow_builder.input_value.id}
                )

                # Create output values
                related_outputs = static_metadata.get_output(
                    input_name=_input_param, input_value=_input_value
                )
                if not related_outputs:
                    logger.warning(
                        f"No related output artifacts found for input (name: {_input_param}, "
                        f"value: {_input_value})"
                    )
                    continue
                for _output in related_outputs:
                    logger.debug(
                        f"Adding output value to retrospective provenance: {_output}"
                    )
                    _output_value_properties = {
                        "identifier": _output["name"],
                        "@type": "Dataset"
                        if Path(_output["source_path"]).is_dir()
                        else "File",
                        "source_path": _output.get("source_path", None),
                        "dest_path": _output.get("dest_path", None),
                        "description": _output.get("description", None),
                        "license": _output.get("license", None)
                        or workflow_builder.crate.license,
                    }
                    # Roles
                    for org_data in static_metadata.orgs:
                        role = org_data.get("role", None)
                        if role:
                            _output_value_properties[role] = org_data["full_name"]
                    # Extra properties per @type
                    if _output_value_properties["@type"] in ["File", "Dataset"]:
                        _output_value_properties.update(
                            {
                                "encodingFormat": ProvenanceMetadataUtils.get_media_format(
                                    _output["source_path"]
                                ),
                                "size": {
                                    "@id": f"#{_output_value_properties['identifier']}-size"
                                },
                            }
                        )
                        workflow_builder.add_entity_size(
                            {
                                "@id": f"#{_output_value_properties['identifier']}-size",
                                "@type": "QuantitativeValue",
                                "value": os.path.getsize(_output["source_path"]),
                                "unitCode": "bytes",
                            }
                        )

                    workflow_builder.output_value = _output_value_properties
                    workflow_builder.action.append_to(
                        "result", {"@id": workflow_builder.output_value.id}
                    )
                    # Add output result to main CreateAction
                    create_action_main.append_to(
                        "result", {"@id": workflow_builder.output_value.id}
                    )
            step_run_no += 1


class ProvenanceTracker:
    def __init__(
        self,
        workflow_spec: str,
        workflow_spec_ignore_step: tuple,
        static_metadata: Union[str, ProvenanceTrackerStaticInfo] = None,
        rocrate_profile: str = None,
        rocrate_gen_preview: bool = False,
        output_crate_path: str = "./crate",
        output_crate_zip: str = None,
    ):
        self.workflow_spec = workflow_spec
        self.workflow_spec_ignore_step = workflow_spec_ignore_step
        self.rocrate_profile = rocrate_profile
        self.rocrate_gen_preview = rocrate_gen_preview
        self.output_crate_path = output_crate_path
        self.output_crate_zip = output_crate_zip
        self.static_metadata = None
        if static_metadata:
            if isinstance(static_metadata, ProvenanceTrackerStaticInfo):
                self.static_metadata = static_metadata
            else:
                self.static_metadata = ProvenanceTrackerStaticInfo(
                    static_metadata_file=static_metadata
                )
        self.workflow_builder = self._init_builder()
        self.workflow_reader = self._init_reader()

    def _init_builder(self):
        _workflow_builder = WorkflowBuilder(
            name=self.workflow_spec,
            path=self.workflow_spec,
            rocrate_profile=self.rocrate_profile,
            rocrate_gen_preview=self.rocrate_gen_preview,
            authors=self.static_metadata.authors,
            orgs=self.static_metadata.orgs,
        )
        # add license
        _workflow_builder.crate.license = ProvenanceMetadataUtils.get_license()
        if self.static_metadata.license:
            _workflow_builder.crate.license = self.static_metadata.license

        logger.debug("Created new RO-Crate and WorkflowBuilder instances")

        return _workflow_builder

    def _init_reader(self):
        argo_workflow_reader = ArgoDefinitionReader(
            self.workflow_builder.workflow_path,
            ignore_steps=self.workflow_spec_ignore_step,
        )
        logger.debug("Loaded workflow reader of type Argo")

        return argo_workflow_reader

    def run(self):
        # Validate static metadata
        self.static_metadata.validate(check_path_presence=True)

        # If provided, add static metadata file to the crate folder
        if self.static_metadata.config_file:
            self.workflow_builder.add_file(
                {
                    "@id": self.static_metadata.config_file,
                    "description": "File containing the initial provenance metadata",
                    "source_path": self.static_metadata.config_file,
                    "dest_path": Path(self.static_metadata.config_file).name,
                }
            )
            logger.debug(f"Added {self.static_metadata.config_file} file to the crate")

        # Prospective provenance
        prospective_provenance(
            self.workflow_builder, self.static_metadata, self.workflow_reader
        )
        # Retrospective provenance
        retrospective_provenance(
            self.workflow_builder, self.static_metadata, self.workflow_reader
        )

        # Save the crate folder
        if self.output_crate_path:
            logger.info(f"Saving RO-Crate to: {self.output_crate_path}")
            self.workflow_builder.crate.write_crate(self.output_crate_path)
            abs_output_path = os.path.abspath(self.output_crate_path)
            logger.debug(f"RO-Crate successfully stored at: {abs_output_path}")
        # Save the crate in zip format
        if self.output_crate_zip:
            logger.debug(f"Saving RO-Crate as a zip file: {self.output_crate_zip}")
            zip_path = Path(self.output_crate_zip)
            if not zip_path.is_absolute():
                zip_path = Path("./").resolve() / zip_path
            logger.debug(f"Saving RO-Crate in zip format to '{zip_path}'")
            self.workflow_builder.crate.write_zip(zip_path.as_posix())
            logger.info(f"RO-Crate in zip format successfully stored at '{zip_path}'")
        # Notify whether the HTML crate has been generated
        if self.workflow_builder.crate.preview:
            logger.info("HTML crate generated in crate folder: ro-crate-preview.html")

        return (self.output_crate_path, self.output_crate_zip)
