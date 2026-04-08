import json
import logging
import os
import uuid
from pathlib import Path

from pythonjsonlogger.jsonlogger import JsonFormatter

logger = logging.getLogger(__name__)

REQUEST_ID: str | None = None

# Constants for provenance actions
START_ACTION = "/start"
STOP_ACTION = "/stop"
CREATE_ACTION = "/create"
CREATE_END_ACTION = "/create_end"
OUTPUT_ACTION = "/output"


def get_request_id() -> str:
    return REQUEST_ID


def generate_request_id():
    global REQUEST_ID
    REQUEST_ID = str(uuid.uuid4())


class ProvFormatter(JsonFormatter):
    """Custom JsonFormatter that adds a global request ID and task name to each log record."""

    def __init__(self, *args, task_name=None, **kwargs):
        self.task_name = task_name
        super().__init__(*args, **kwargs)

    def add_fields(self, log_record, record, message_dict):
        super(ProvFormatter, self).add_fields(log_record, record, message_dict)
        # Add task name
        log_record["task"] = self.task_name
        # Add request_id
        if not log_record.get("request_id"):
            log_record["request_id"] = get_request_id()


class ProvLogger(logging.Logger):
    """
    Custom Logger that sets up the provenance log.

    It uses ProvFormatter as formatter and console and file handlers.

    Parameters
    ----------
    name : str
        Name of the logger.
    task_name : str
        Name of the task that is subject to provenance tracking.
    level : int, optional
        Logging level, by default logging.INFO.
    logfile_path : str, optional
        Path to the log file, by default None (will use '{name}.log').
    """

    def __init__(
        self,
        name: str,
        task_name: str,
        level: int = logging.INFO,
        logfile_path: str = None,
    ):
        super().__init__(name, level)
        # Set log root directory to './logs' within the current path
        log_dir = (Path.cwd() / "./logs").resolve()
        log_dir.mkdir(exist_ok=True)
        logger.debug(f"Created log directory relative to the curent path: {log_dir}")
        # risk mitigation: path traversal
        logging.debug("Mitigating path traversal risk")
        if not logfile_path:
            # "name" shall not contain "../"
            name_validated = Path(name).name
            log_path = log_dir / f"{name_validated}.log"
            logger.debug(
                "Log path was not provided, using a validated version of log's name: {log_path}"
            )
        else:
            log_path = Path(logfile_path).resolve()
        logfile_path_resolved = Path(logfile_path).resolve() / "./logs"
        logger.debug(
            f"Log path provided, resolving log path to '{logfile_path_resolved}'"
        )
        if not logfile_path_resolved.is_relative_to(log_dir):
            raise ValueError(
                "Wrong log path provided: not within the allowed directory "
                "(requested: {logfile_path}, allowed: {log_dir})"
            )
        # risk migitation: principle of least privilege and symlink attack
        logging.debug("Mitigating least privilege and symlink attack risks")
        if log_path.is_symlink():
            raise IOError(f"Security risk: {log_path} is a symlink.")

        if not log_path.exists():
            logger.debug(f"Creating log file: {log_path}")
            log_path.touch(mode=0o600)
        else:
            logger.debug(f"Log file exists, setting permissions: {log_path}")
            os.chmod(log_path, 0o600)
        logger.debug(
            "Setting owner's read&write permissions to the provenance log file: {log_path}"
        )
        # Generate request ID
        generate_request_id()
        # Unique formatter
        formatter = ProvFormatter(task_name=task_name, timestamp=True)
        # Two handlers: console and file
        ## console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        self.addHandler(console_handler)
        ## file handler
        if not logfile_path:
            logfile_path = f"{name}.log"
        file_handler = logging.FileHandler(logfile_path)
        file_handler.setFormatter(formatter)
        self.addHandler(file_handler)
        self.propagate = False


class ProvenanceLog:
    """
    Provenance log reader class to parse and extract information from a provenance log file.

    Parameters
    ----------
    logfile_path : str
        Path to the provenance log file.
    request_id : str, optional
        Specific request ID to filter log entries, by default None (will use the last request ID
        in the log file).
    output_types : list, optional
        List of output types to extract from the log, by default None (will not extract any outputs).
    """

    def __init__(self, logfile_path: str, output_types: list = None):
        self.__logfile_path = logfile_path
        self.__task = None
        self.__data = self.get_logs(logfile_path)

    @property
    def task(self):
        return self.__task

    @property
    def data(self):
        """
        Getter method to extract the log entries.

        Returns
        -------
        list (of dict)
            List of log entries.
        """
        return self.__data

    def get_logs(self, logfile_path: str) -> list:
        """
        Retrieve log entries from the provenance log file,filtered by request ID if provided.

        When request ID is not provided, the method seeks and uses the last request ID found in the log file.

        Parameters
        ----------
        logfile_path : str
            Path to the provenance log file.

        Returns
        -------
        list (of dict)
            List of log entries filtered by request ID.
        """
        log_entries = {}
        with open(logfile_path, "r") as f:
            _request_id = None
            _task = None
            for line_number, line in enumerate(f, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    log_entry = json.loads(line)
                except json.JSONDecodeError as e:
                    raise ValueError(f"Error parsing line {line_number}: {e}")
                if not _task:
                    self.__task = log_entry.get("task", "")
                    if not self.__task:
                        logger.warning(
                            f"Task not defined in provenance log ('task' property): {self.__logfile_path}"
                        )
                if not _request_id or _request_id != log_entry.get("request_id"):
                    _request_id = log_entry.get("request_id")
                    log_entries[_request_id] = []
                log_entries[_request_id].append(log_entry)
            self.__run_ids = log_entries.keys()
        return log_entries

    def get_entries_for_key(
        self,
        key_name: str,
        action: str = None,
        whole_entry: bool = False,
        entry_list: list = None,
    ) -> list:
        """
        Retrieve the value or the whole entry for a specific key from the log entries.

        The results are optionally filtered by action and entry list.

        Parameters
        ----------
        key_name : str
            Key name to search for in the log entries.
        action : str, optional
            Specific action to filter log entries, by default None (will search in all entries).
        whole_entry : bool, optional
            If True, returns the whole log entry instead of just the value for the key, by
            default False.
        entry_list : list, optional
            Specific list of log entries to search in, by default None (will search in all logs).

        Returns
        -------
        list
            List of values for the specified key name from the log entries.

        """
        values_for_key = []
        if entry_list:
            logs_to_search = entry_list
            logger.debug(
                f"Scoping the search to {len(logs_to_search)} entries: {logs_to_search}"
            )
        else:
            logs_to_search = self.__logs
        for entry in logs_to_search:
            if action and entry.get("action") != action:
                continue
            if whole_entry:
                values_for_key.append(entry)
                continue
            value = entry.get(key_name, None)
            if value and isinstance(value, list):
                values_for_key.extend(value)
            elif value:
                values_for_key.append(value)
        if not values_for_key:
            log_message = f"No entries found for key '{key_name}'"
            if action:
                log_message += f" with action '{action}'"
            raise ValueError(log_message)
        return values_for_key

    def get_input_args(self):
        input_args_data = {}
        for run_id, run_entries in self.data.items():
            input_args_data[run_id] = self.get_entries_for_key(
                "input_args", action="/input", entry_list=run_entries
            )
        return input_args_data

    def get_outputs(self):
        output_data = {}
        for run_id, run_entries in self.data.items():
            output_data[run_id] = self.get_entries_for_key(
                "output_files", action="/output", entry_list=run_entries
            )
        return output_data

    @property
    def workflow_file(self):
        """
        Getter method to extract the workflow script file path.

        Returns
        -------
        str
            Path to the workflow script file.
        """
        return self.get_entries_for_key("workflow_file", action=START_ACTION)[-1]

    @property
    def programming_language(self):
        """
        Getter method to extract the programming language used in the workflow.

        Returns
        -------
        str
            Programming language used in the workflow.
        """
        return self.get_entries_for_key("python_version", action=START_ACTION)[-1]

    @property
    def actions(self):
        """
        Getter method to extract the actions from the log entries.

        Returns
        -------
        dict
            Dictionary with action IDs as keys and list of log entries as values.
        """
        action_entries_dict = {}
        inside = False
        action_id = None
        for entry in self.__logs:
            action = entry.get("action")
            logger.debug(f"Processing log entry: {entry}")

            if action == CREATE_ACTION:
                if inside:
                    raise ValueError(
                        "Malformed provenance log: Found an action=create without previous action=create_end."
                    )
                action_id = entry.get("action_id")
                if not action_id:
                    raise KeyError(
                        "Malformed provenance log: No action_id found in action=create entry."
                    )
                inside = True
                action_entries_dict[action_id] = [entry]
                continue

            if not inside:
                continue

            action_entries_dict[action_id].append(entry)
            logger.debug(f"Appending entry to action_id {action_id}: {entry}")

            if action == CREATE_END_ACTION:
                inside = False
                logger.debug(
                    f"Rolling out: Ending action_id {action_id} entries collection."
                )

        return action_entries_dict

    @property
    def inputs(self):
        """
        Getter method to extract the input arguments from the log entries.

        Returns
        -------
        list
            List of input arguments.
        """
        return self.get_entries_for_key("input_args")

    @property
    def outputs(self):
        """
        Getter method to extract the output entries from the log entries.

        Returns
        -------
        dict
            Dictionary with output types as keys and list of output entries as values.
        """
        output_entries_per_type = {}
        for output_type in self.__output_types:
            entries = self.get_entries_for_key(output_type, action=OUTPUT_ACTION)
            logger.debug(
                f"Retrieved {len(entries)} entries for output type '{output_type}': {entries}"
            )
            if not entries:
                raise ValueError(f"Unsupported output type requested: {output_type}")
            output_entries_per_type[output_type] = entries
        return output_entries_per_type

    @property
    def start_time(self):
        """
        Getter method to extract the start time of the workflow.

        Returns
        -------
        list
            List of start time entries.
        """
        return self.get_entries_for_key("timestamp", action=START_ACTION)

    @property
    def end_time(self):
        """
        Getter method to extract the end time of the workflow.

        Returns
        -------
        list
            List of end time entries.
        """
        return self.get_entries_for_key("timestamp", action=STOP_ACTION)
