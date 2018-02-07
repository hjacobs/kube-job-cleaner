#!/usr/bin/env python3

import argparse
import datetime
import os
import pykube
import time


def parse_time(s: str):
    return datetime.datetime.strptime(s, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=datetime.timezone.utc).timestamp()


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

now = time.time()
for job in pykube.Job.objects(api, namespace=pykube.all):
    completion_time = job.obj['status'].get('completionTime')
    status = job.obj['status']

    if (status.get('succeeded') or status.get('failed')) and completion_time:
        completion_time = parse_time(completion_time)
        seconds_since_completion = now - completion_time
        if seconds_since_completion > args.seconds:
            print('Deleting {} ({:.0f}s old)..'.format(job.name, seconds_since_completion))
            if args.dry_run:
                print('** DRY RUN **')
            else:
                job.delete()
            continue

    start_time = job.obj['status'].get('startTime')
    if start_time:
        seconds_since_start = now - parse_time(start_time)

        # Determine the timeout in seconds for this job
        annotations = job.obj['metadata'].get('annotations', {})
        cleanup_timeout = int(annotations.get('cleanup-timeout', args.timeout_seconds))

        # Check whether a timeout is active for this job.
        if cleanup_timeout < 0:
            continue

        if start_time and seconds_since_start > cleanup_timeout:
            print('Deleting Job because of timeout {} ({:.0f}s running)..'.format(job.name, seconds_since_start))
            if args.dry_run:
                print('** DRY RUN **')
            else:
                job.delete()

for pod in pykube.Pod.objects(api, namespace=pykube.all):
    if pod.obj['status'].get('phase') in ('Succeeded', 'Failed'):
        seconds_since_completion = 0
        if pod.obj['status'].get('containerStatuses') is None:
            print("Warning: Skipping pod without containers ({})".format(pod.obj['metadata'].get('name')))
            continue
        for container in pod.obj['status'].get('containerStatuses'):
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

        if seconds_since_completion > args.seconds:
            print('Deleting {} ({:.0f}s old)..'.format(pod.name, seconds_since_completion))
            if args.dry_run:
                print('** DRY RUN **')
            else:
                pod.delete()
