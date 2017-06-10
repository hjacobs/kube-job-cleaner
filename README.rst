======================
Kubernetes Job Cleaner
======================

Very simple script to delete all completed jobs after X seconds (default: one hour).

Kubernetes jobs are not cleaned up by default and completed pods are never deleted.
Running jobs frequently (like every few minutes) quickly thrashes the Kubernetes API server with unnecessary pod resources. A significant slowdown of the API server can be observed with increasing number of completed jobs/pods hanging around.
To mitigate this, This small ``kube-job-cleaner`` script runs as a CronJob every hour and cleans up completed jobs/pods.

See `Zalando's Running Kubernetes in Production document <https://kubernetes-on-aws.readthedocs.io/en/latest/admin-guide/kubernetes-in-production.html>`_ for more information.

Building the Docker image:

.. code-block:: bash

    $ make

Deploying:

.. code-block:: bash

    $ kubectl apply -f deploy/

There are a few options:

``--seconds``
    Number of seconds after job completion to remove the job (default: 1 hour)
``--timeout-seconds``
    Kill all jobs after X seconds (default: never)
``--dry-run``
    Do not actually remove any jobs, only print what would be done
