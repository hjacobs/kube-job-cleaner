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
    start_time = parse_time(job.obj['status'].get('startTime'))
    seconds_since_start = now - start_time
    annotations = job.obj['metadata'].get('annotations')
    # Determine the timeout in seconds for this job
    timeout_jobs = args.timeout_seconds
    if annotations is not None:
        timeout_override = annotations.get('cleanup-timeout')
        if timeout_override is not None:
            timeout_jobs = int(timeout_override)
    # Check whether a timeout is active for this job.
    if timeout_jobs < 0:
        continue
    if start_time and seconds_since_start > timeout_jobs:
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
