======================
Kubernetes Job Cleaner
======================

Very simple script to delete all completed jobs after X seconds (default: one hour).

Building the Docker image:

.. code-block:: bash

    $ make

Deploying:

.. code-block:: bash

    $ kubectl apply -f deploy/


