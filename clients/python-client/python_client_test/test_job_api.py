import time
import unittest
from python_client import kuberay_job_api
from python_client import kuberay_cluster_api
from python_client.utils import kuberay_cluster_builder
from python_client import constants


class TestUtils(unittest.TestCase):
    def __init__(self, methodName: str = ...) -> None:
        super().__init__(methodName)

        self.api = kuberay_job_api.RayjobApi()
        self.cluster_api = kuberay_cluster_api.RayClusterApi()
        self.director = kuberay_cluster_builder.Director()

    def test_submit_ray_job_with_existing_cluster(self):
        """Test submitting a job to an existing cluster using clusterSelector."""
        # Create a cluster using the director
        cluster_name = "premade"
        namespace = "default"

        # Build a small cluster
        cluster_body = self.director.build_small_cluster(
            name=cluster_name,
            k8s_namespace=namespace,
            labels={"ray.io/cluster": cluster_name},
        )

        # Ensure cluster was built successfully
        self.assertIsNotNone(cluster_body, "Cluster should be built successfully")
        self.assertEqual(cluster_body["metadata"]["name"], cluster_name)

        # Create the cluster in Kubernetes
        created_cluster = self.cluster_api.create_ray_cluster(
            body=cluster_body, k8s_namespace=namespace
        )

        self.assertIsNotNone(created_cluster, "Cluster should be created successfully")

        self.cluster_api.wait_until_ray_cluster_running(cluster_name, namespace, 60, 10)
        job_name = "premade-cluster-job"
        try:
            # Create job spec with clusterSelector
            job_body = {
                "apiVersion": constants.GROUP + "/" + constants.JOB_VERSION,
                "kind": constants.JOB_KIND,
                "metadata": {
                    "name": job_name,
                    "namespace": namespace,
                    "labels": {
                        "app.kubernetes.io/name": job_name,
                        "app.kubernetes.io/managed-by": "kuberay",
                    },
                },
                "spec": {
                    "clusterSelector": {
                        "ray.io/cluster": cluster_name,
                    },
                    "entrypoint": 'python -c "import time; time.sleep(20)"',
                    "submissionMode": "K8sJobMode",
                },
            }

            submitted_job = self.api.submit_job(
                job=job_body,
                k8s_namespace=namespace,
            )

            self.assertIsNotNone(submitted_job, "Job should be submitted successfully")
            self.assertEqual(submitted_job["metadata"]["name"], job_name)
            self.assertEqual(
                submitted_job["spec"]["clusterSelector"]["ray.io/cluster"], cluster_name
            )

            self.api.wait_until_job_finished(job_name, namespace, 120, 10)
        finally:
            self.cluster_api.delete_ray_cluster(
                name=cluster_name, k8s_namespace=namespace
            )

            self.api.delete_job(job_name, namespace)

    def test_submit_ray_job_no_cluster(self):
        """Test submitting a job without a cluster"""
        namespace = "default"

        job_name = "no-cluster-job"
        try:
            # Create job spec with clusterSelector
            job_body = {
                "apiVersion": constants.GROUP + "/" + constants.JOB_VERSION,
                "kind": constants.JOB_KIND,
                "metadata": {
                    "name": job_name,
                    "namespace": namespace,
                    "labels": {
                        "app.kubernetes.io/name": job_name,
                        "app.kubernetes.io/managed-by": "kuberay",
                    },
                },
                "spec": {
                    "entrypoint": 'python -c "import time; time.sleep(20)"',
                    "submissionMode": "K8sJobMode",
                    "shutdownAfterJobFinishes": True,
                    "rayClusterSpec": {
                        "rayVersion": "2.46.0",
                        "headGroupSpec": {
                            "rayStartParams": {
                                "dashboard-host": "0.0.0.0",
                            },
                            "template": {
                                "spec": {
                                    "containers": [
                                        {
                                            "name": "ray-head",
                                            "image": "rayproject/ray:2.46.0",
                                            "ports": [
                                                {
                                                    "containerPort": 6379,
                                                    "name": "gcs-server",
                                                },
                                                {
                                                    "containerPort": 8265,
                                                    "name": "dashboard",
                                                },
                                                {
                                                    "containerPort": 10001,
                                                    "name": "client",
                                                },
                                            ],
                                            "resources": {
                                                "limits": {"cpu": "1", "memory": "2Gi"},
                                                "requests": {
                                                    "cpu": "1",
                                                    "memory": "1Gi",
                                                },
                                            },
                                        }
                                    ],
                                },
                            },
                        },
                        "workerGroupSpecs": [
                            {
                                "replicas": 2,
                                "minReplicas": 1,
                                "maxReplicas": 2,
                                "groupName": "sg",
                                "rayStartParams": {},
                                "template": {
                                    "spec": {
                                        "containers": [
                                            {
                                                "name": "ray-worker",
                                                "image": "rayproject/ray:2.46.0",
                                                "resources": {
                                                    "limits": {
                                                        "cpu": "1",
                                                        "memory": "2Gi",
                                                    },
                                                    "requests": {
                                                        "cpu": "1",
                                                        "memory": "1Gi",
                                                    },
                                                },
                                            }
                                        ]
                                    }
                                },
                            }
                        ],
                    },
                },
            }
            submitted_job = self.api.submit_job(
                job=job_body,
                k8s_namespace=namespace,
            )

            self.assertIsNotNone(submitted_job, "Job should be submitted successfully")
            self.assertEqual(submitted_job["metadata"]["name"], job_name)

            self.api.wait_until_job_finished(job_name, namespace, 120, 10)
        finally:
            self.api.delete_job(job_name, namespace)

    def test_get_ray_job(self):
        # TODO Create a ray job and assert that the get_ray_job returns the correct job
        ray_job = self.api.get_job(name="simple-rayjob", k8s_namespace="bkeane")
        self.assertNotEqual(ray_job, None)

    def test_list_ray_jobs(self):
        # TODO Create 1-3 ray jobs and assert that the list_ray_jobs returns the correct number of jobs and their details are correct etc
        ray_jobs = self.api.list_jobs(k8s_namespace="bkeane")
        self.assertNotEqual(ray_jobs, None)
        self.assertGreater(len(ray_jobs), 0)
