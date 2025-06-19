"""
Set of APIs to manage rayjobs.
"""

import copy
import logging
import time
from kubernetes import client, config
from kubernetes.client.rest import ApiException
from typing import Any, Dict, List, Optional
from python_client import constants


log = logging.getLogger(__name__)
if logging.getLevelName(log.level) == "NOTSET":
    logging.basicConfig(format="%(asctime)s %(message)s", level=constants.LOGLEVEL)

TERMINAL_JOB_STATUSES = [
    "STOPPED",
    "SUCCEEDED",
    "FAILED",
]


class RayjobApi:
    """
    RayjobApi provides APIs to list, get, create, build, update, delete rayjobs.

    Methods:
    - submit_job(entrypoint: str, ...) -> str: Submit and execute a job asynchronously.
    - stop_job(job_id: str) -> (bool, str): Request a job to exit asynchronously.
    - get_job_status(job_id: str) -> str: Get the most recent status of a job.
    - wait_until_job_finished(job_id: str) -> bool: Wait until a job is completed.
    - get_job_info(job_id: str): Get the latest status and other information associated with a job.
    - list_jobs() -> List[JobDetails]: List all jobs along with their status and other information.
    - get_job_logs(job_id: str) -> str: Get all logs produced by a job.
    - tail_job_logs(job_id: str) -> Iterator[str]: Get an iterator that follows the logs of a job.
    - delete_job(job_id: str) -> (bool, str): Delete a job in a terminal state and all of its associated data.
    """

    # initial config to setup the kube client
    def __init__(self):
        # loading the config
        self.kube_config: Optional[Any] = config.load_kube_config()
        self.api = client.CustomObjectsApi()

    def __del__(self):
        self.api = None
        self.kube_config = None

    def submit_job(self, k8s_namespace: str = "default", job: Any = None) -> Any:
        """Submit a Ray job to a given namespace."""
        try:
            rayjob = self.api.create_namespaced_custom_object(
                group=constants.GROUP,
                version=constants.JOB_VERSION,
                plural=constants.JOB_PLURAL,
                body=job,
                namespace=k8s_namespace,
            )
            return rayjob
        except ApiException as e:
            log.error("error submitting ray job: {}".format(e))
            return None

    def stop_job(self, job_id: str) -> (bool, str):
        """Request a job to exit asynchronously."""
        return False, ""

    def get_job_status(
        self,
        name: str,
        k8s_namespace: str = "default",
        timeout: int = 60,
        delay_between_attempts: int = 5,
    ) -> Any:
        while timeout > 0:
            try:
                resource: Any = self.api.get_namespaced_custom_object_status(
                    group=constants.GROUP,
                    version=constants.JOB_VERSION,
                    plural=constants.JOB_PLURAL,
                    name=name,
                    namespace=k8s_namespace,
                )
            except ApiException as e:
                if e.status == 404:
                    log.error("rayjob resource is not found. error = {}".format(e))
                    return None
                else:
                    log.error("error fetching custom resource: {}".format(e))
                    return None

            if resource and "status" in resource and resource["status"]:
                return resource["status"]
            else:
                log.info("rayjob {} status not set yet, waiting...".format(name))
                time.sleep(delay_between_attempts)
                timeout -= delay_between_attempts

        log.info("rayjob {} status not set yet, timing out...".format(name))
        return None

    def wait_until_job_finished(
        self,
        name: str,
        k8s_namespace: str = "default",
        timeout: int = 60,
        delay_between_attempts: int = 5,
    ) -> bool:
        status = self.get_job_status(
            name, k8s_namespace, timeout, delay_between_attempts
        )

        while timeout > 0:
            status = self.get_job_status(
                name, k8s_namespace, timeout, delay_between_attempts
            )

            if (
                "jobStatus" in status
                and status["jobStatus"].upper() in TERMINAL_JOB_STATUSES
            ):
                log.info(
                    "raycluster {} is ready with state: {}".format(
                        name, status["jobStatus"]
                    )
                )
                return True
            else:
                log.info(
                    "raycluster {} status is not ready yet, current status is {}".format(
                        name,
                        status["jobStatus"] if "jobStatus" in status else "unknown",
                    )
                )

            time.sleep(delay_between_attempts)
            timeout -= delay_between_attempts

    def list_jobs(
        self,
        k8s_namespace: str = "default",
        label_selector: str = "",
        async_req: bool = False,
    ) -> Any:
        try:
            resources = self.api.list_namespaced_custom_object(
                group=constants.GROUP,
                version=constants.JOB_VERSION,
                plural=constants.JOB_PLURAL,
                namespace=k8s_namespace,
                label_selector=label_selector,
                async_req=async_req,
            )
            return resources
        except ApiException as e:
            log.error("error listing ray jobs: {}".format(e))
            return None

    def get_job(self, name: str, k8s_namespace: str = "default") -> Any:
        try:
            resource: Any = self.api.get_namespaced_custom_object(
                group=constants.GROUP,
                version=constants.JOB_VERSION,
                plural=constants.JOB_PLURAL,
                name=name,
                namespace=k8s_namespace,
            )
            return resource
        except ApiException as e:
            if e.status == 404:
                log.error("rayjob resource is not found. error = {}".format(e))
                return None
            else:
                log.error("error fetching custom resource: {}".format(e))
                return None

    def delete_job(self, name: str, k8s_namespace: str = "default") -> bool:
        try:
            resource: Any = self.api.delete_namespaced_custom_object(
                group=constants.GROUP,
                version=constants.JOB_VERSION,
                plural=constants.JOB_PLURAL,
                name=name,
                namespace=k8s_namespace,
            )
            return resource
        except ApiException as e:
            if e.status == 404:
                log.error(
                    "rayjob custom resource already deleted. error = {}".format(
                        e.reason
                    )
                )
                return None
            else:
                log.error(
                    "error deleting the rayjob custom resource: {}".format(e.reason)
                )
                return None

    # def patch_job(
    #     self, name: str, ray_patch: Any, k8s_namespace: str = "default"
    # ) -> Any:
    #     return None
