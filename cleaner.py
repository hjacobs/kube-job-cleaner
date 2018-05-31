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


def pod_expired(max_age, pod):
    now = time.time()
    pod_status = pod.obj['status']

    if pod_status.get('phase') in ('Succeeded', 'Failed'):
        container_statuses = pod_status.get('containerStatuses', [])

        if pod_status.get('reason') == 'Preempting':
            # preempting pods don't have any container information, so let's remove them immediately
            return 'preempted'
        elif not container_statuses:
            print("Warning: Skipping pod without containers ({})".format(pod.obj['metadata'].get('name')))
            return
        else:
            seconds_since_completion = 0
            for container in pod_status.get('containerStatuses'):
                if 'terminated' in container['state']:
                    state = container['state']
                elif 'terminated' in container.get('lastState', {}):
                    # current state might be "waiting", but lastState is good enough
                    state = container['lastState']
                else:
                    state = None
                if state:
                    finish = now - parse_time(state['terminated']['finishedAt'])
                    if seconds_since_completion == 0 or finish < seconds_since_completion:
                        seconds_since_completion = finish

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
    args = parser.parse_args()

    try:
        config = pykube.KubeConfig.from_service_account()
    except FileNotFoundError:
        # local testing
        config = pykube.KubeConfig.from_file(os.path.expanduser('~/.kube/config'))
    api = pykube.HTTPClient(config)

    for job in pykube.Job.objects(api, namespace=pykube.all):
        delete_if_expired(args.dry_run, job, job_expired(args.seconds, args.timeout_seconds, job))

    for pod in pykube.Pod.objects(api, namespace=pykube.all):
        delete_if_expired(args.dry_run, pod, pod_expired(args.seconds, pod))


if __name__ == "__main__":
    main()
