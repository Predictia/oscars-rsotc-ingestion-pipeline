import hashlib
import logging
import os
from abc import ABC, abstractmethod

from dotenv import load_dotenv
from hera.workflows import WorkflowsService

logger = logging.getLogger(__name__)


class WorkflowEngine(ABC):
    """Blueprint class to execute a workflow and extract information from the execution."""

    @abstractmethod
    def _connect(self):
        """Connect to the workflow endpoint."""
        pass


class ArgoWorkflowEngine(WorkflowEngine):
    """
    Information provider for Argo workflow executions.

    Parameters
    ----------
    endpoint : str
        Argo workflow endpoint.
    """

    def __init__(self, name: str = None, namespace: str = None):
        self.workflow_name = name
        self.workflow_namespace = namespace
        self.api_client = self._connect()
        try:
            self.workflow_obj = self.api_client.get_workflow(
                self.workflow_name, self.workflow_namespace
            )
        except Exception as e:
            raise Exception(f"Failed to get workflow from Argo server: {str(e)}")

    def _connect(self):
        """Connect to the Argo workflow endpoint."""
        load_dotenv()

        # Load class attributes first
        self.workflow_name = self.workflow_namespace or os.getenv("ARGO_WORKFLOW")
        self.workflow_namespace = self.workflow_namespace or os.getenv("ARGO_NAMESPACE")

        # Retrieve Argo configuration
        argo_config = {
            "argo_server": os.getenv("ARGO_SERVER"),
            "argo_token": os.getenv("ARGO_TOKEN"),
            "argo_namespace": self.workflow_namespace,
            "argo_workflow": self.workflow_name,
            "argo_verify_ssl": False
            if os.getenv("ARGO_INSECURE_SKIP_VERIFY") == "true"
            else True,
        }

        # Connect to Argo
        connection_params = {
            "host": argo_config["argo_server"],
            "verify_ssl": argo_config["argo_verify_ssl"],
        }
        if argo_config.get("argo_token", None):
            connection_params["token"] = argo_config["argo_token"]
            connection_params_copy = connection_params.copy()
            connection_params_copy["token"] = hashlib.sha256(
                connection_params["token"].encode()
            ).hexdigest()
        logger.info(f"Passing connection parameters to Argo: {connection_params_copy}")

        return WorkflowsService(**connection_params)

    def _get_status(self):
        """Get the status of the workflow execution."""
        return self.workflow_obj.status

    def get_parameters(self, workflow_level=True):
        """Get the parameters of the workflow execution.

        Parameters
        ----------
        workflow_level : bool
            Return only the input arguments at the level of workflow
            (e.g. {{workflow.parameters.<name-of-the-input-parameter}})
        """
        if workflow_level:
            return dict(
                [
                    [param.name, param.value]
                    for param in self.workflow_obj.spec.arguments.parameters
                ]
            )

    def get_main_workflow_data(self):
        return self._get_status()

    def get_execution_data(self, tool_name: str):
        # Tool executions relate to "type: Pod"
        return dict(
            [
                [execution_id, execution_data]
                for execution_id, execution_data in self._get_status().nodes.items()
                if execution_data.type == "Pod"
                and execution_data.template_name == tool_name
            ]
        )

    def get_execution_parameters(self, execution_data: dict):
        return dict(
            [
                [parameter.name, parameter.value]
                for parameter in execution_data.inputs.parameters
            ]
        )
