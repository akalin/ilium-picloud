import argparse
import cloud
import io
from os import path
import re
import subprocess
import sys


def run_ilium(config_path, output_dir, i, jobs):
    '''Shell out to ilium to render.
    '''
    result_str = subprocess.check_call(
        ['/home/picloud/src/go/bin/ilium',
         '-d=%s' % output_dir,
         '-j=%d' % jobs,
         '-x=%d' % i,
         config_path])


def main():
    parser = argparse.ArgumentParser(
        description='Render using ilium on picloud.')
    parser.add_argument('-j', type=int, help='the number of jobs to use')
    parser.add_argument('-c', type=int,
                        help='the number of cores per job to use')
    parser.add_argument('config_path',
                        help='the path to the ilium config to use')
    args = parser.parse_args()

    if not args.j:
        args.j = 8

    if args.c < 1:
        args.c = 4

    config_basename = path.basename(args.config_path)
    # TODO(akalin): Append random string.
    job_id = path.splitext(config_basename)[0]
    prefix = job_id + '/'
    bucket_dir = path.join('/bucket', prefix)
    bucket_config_path = path.join(bucket_dir, config_basename)

    def run_ilium_for_job(i):
        return run_ilium(bucket_config_path, bucket_dir, i, args.c)

    cloud.bucket.put(args.config_path, config_basename, prefix=prefix)

    print 'calling into PiCloud with %d jobs and %d cores...' % (
        args.j, args.c)
    jids = cloud.map(run_ilium_for_job, xrange(0, args.j),
                     _env='ilium', _type='f2', _cores=args.c,
                     _label='run_ilium(%s)' % job_id)

    remaining_jobs = len(jids)
    print ('waiting for results from %d jobs starting from %d...' %
           (remaining_jobs, jids[0]))
    results = cloud.iresult(jids)
    for result in results:
        remaining_jobs -= 1
        print 'job done; still waiting for %d jobs...' % remaining_jobs

    bin_files = []

    for bucket_path in cloud.bucket.iterlist(prefix=prefix):
        if ".bin" not in bucket_path:
            continue

        print 'getting file %s' % bucket_path
        cloud.bucket.get(bucket_path, bucket_path)
        bin_files.append(bucket_path)

    # TODO(akalin): Download files while waiting for results.
    # TODO(akalin): Handle groups of .bin files.
    if bin_files:
        bin_dirname = path.dirname(bin_files[0])
        bin_basename = path.basename(bin_files[0])
        output_file = re.sub(r'\.bin.*', '.png', bin_basename)
        output_path = path.join(bin_dirname, output_file)
        print 'writing output to %s' % output_path
        subprocess.check_call(
            ['/Users/akalin/src/go/bin/ilium-merge-bin-files',
             '-o=%s' % output_path] + bin_files)
    else:
        print 'no .bin files downloaded'


if __name__ == '__main__':
    main()
