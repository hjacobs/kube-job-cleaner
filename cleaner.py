#!/usr/bin/env python3

import argparse
import datetime
import os
import pykube
import time


def parse_time(s: str):
    return datetime.datetime.strptime(s, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=datetime.timezone.utc).timestamp()


def job_expired(max_age, timeout_seconds, job):
    now = time.time()
    status = job.obj['status']

    completion_time = None

    if status.get('succeeded') or status.get('failed'):
        completion_time = status.get('completionTime')
    elif not status:
        # this can happen if the image policy webhook prevents job pods
        # from being created, fall back to creationTimestamp
        completion_time = job.obj['metadata']['creationTimestamp']

    if completion_time:
        completion_time = parse_time(completion_time)
        seconds_since_completion = now - completion_time
        if seconds_since_completion > max_age:
            return '{:.0f}s old'.format(seconds_since_completion)

    start_time = status.get('startTime')
    if start_time:
        seconds_since_start = now - parse_time(start_time)

        # Determine the timeout in seconds for this job
        annotations = job.obj['metadata'].get('annotations', {})
        cleanup_timeout = int(annotations.get('cleanup-timeout', timeout_seconds))

        if cleanup_timeout > 0 and seconds_since_start > cleanup_timeout:
            return 'timeout ({:.0f}s running)'.format(seconds_since_start)


def container_finish_time(status):
    terminated_state = status.get('state', {}).get('terminated') or status.get('lastState')
    if terminated_state:
        finish_time = terminated_state.get('finishedAt')
        if finish_time:
            return parse_time(finish_time)


def termination_time(pod):
    pod_status = pod.obj['status']
    container_statuses = pod_status.get('initContainerStatuses', []) + pod_status.get('containerStatuses', [])
    finish_times = list(filter(None, (container_finish_time(status) for status in container_statuses)))
    if not finish_times:
        return None
    return max(finish_times)


def pod_expired(max_age, pod):
    now = time.time()
    pod_status = pod.obj['status']

    if pod_status.get('phase') in ('Succeeded', 'Failed'):
        if pod_status.get('reason') == 'Preempting':
            # preempting pods don't have any container information, so let's remove them immediately
            return 'preempted'

        # If we cannot determine the finish time, use start time instead
        finish_time = termination_time(pod) or parse_time(pod.obj['metadata']['creationTimestamp'])
        seconds_since_completion = now - finish_time
        if seconds_since_completion > max_age:
            return '{:.0f}s old'.format(seconds_since_completion)


def delete_if_expired(dry_run, entity, reason):
    if reason:
        print("Deleting {} {} ({})".format(entity.kind, entity.name, reason))
        if dry_run:
            print('** DRY RUN **')
        else:
            entity.delete()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--seconds', type=int, default=3600, help='Delete all finished jobs older than ..')
    parser.add_argument('--timeout-seconds', type=int, default=-1, help='Kill all jobs older than ..')
    parser.add_argument('--dry-run', action='store_true', help='Dry run mode')
    parser.add_argument('--namespace', type=str, default=None, help='Only search for completed jobs in a single namespace')
    args = parser.parse_args()

    try:
        config = pykube.KubeConfig.from_service_account()
    except FileNotFoundError:
        # local testing
        config = pykube.KubeConfig.from_file(os.path.expanduser('~/.kube/config'))
    api = pykube.HTTPClient(config)

    namespace = args.namespace or pykube.all

    for job in pykube.Job.objects(api, namespace=namespace):
        delete_if_expired(args.dry_run, job, job_expired(args.seconds, args.timeout_seconds, job))

    for pod in pykube.Pod.objects(api, namespace=namespace):
        delete_if_expired(args.dry_run, pod, pod_expired(args.seconds, pod))


if __name__ == "__main__":
    main()
