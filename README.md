# guppy_job_submitter

This tool is intended for use in breaking up a large number of time-consuming basecalling tasks into a set of CPU-only PBS jobs.

```
usage: prepare_guppy.py [-h] --input_path INPUT_PATH --save_path SAVE_PATH
                       --stage_path STAGE_PATH [--flowcell FLOWCELL]
                       [--kit KIT] [--config CONFIG] [--nsets NSETS]
                       [--njobs NJOBS] [--ppn PPN] [--pergb PERGB]
                       [--walltime WALLTIME]

Submit batches of guppy jobs to PBS scheduler

optional arguments:
  -h, --help            show this help message and exit

required arguments:
  --input_path INPUT_PATH
                        directory containing files to process
  --save_path SAVE_PATH
                        directory to store output files
  --stage_path STAGE_PATH
                        directory to stage calculations

optional arguments:
  --flowcell FLOWCELL   flowcell version
  --kit KIT             sequencing kit version
  --config CONFIG       config file with guppy parameters
  --njobs NJOBS         break calculation into this many PBS jobs (default: 1)
  --nsets NSETS         break calculation into this many subsets (default:
                        determine from file sizes)
  --ppn PPN             processors per node to request for jobs (Default: 24)
  --pergb PERGB         CPU-hours to process 1 GB of files (default: 20, based
                        on a 24 core Mesabi job )
  --walltime WALLTIME   Target real walltime that each job should run. Jobs
                        will request double this time as a buffer. (Default:
                        48)
```

## Installation

This script requires python >= 3.6. If you have that installed, installation of guppy is a matter of downloading the script and adding it to your PATH:

``
git clone https://github.umn.edu/dunn0404/guppy_job_submitter.git

export PATH=$PATH:$PWD/guppy_job_submitter
echo "export PATH=\$PATH:${PWD}/guppy_job_submitter" >> ~/.bashrc
``

## Usage

This script creates a staging directory at the location specified by `--stage_path` to partition the work between jobs and to store the PBS job scripts. No actual data is stored in this staging directory (only references to data files), so you may freely delete it once your calculation is complete, or if you need to re-run this script to change your job parameters.

For most calculations, you will only need to set `--input_path`, `--save_path`, `--stage_path`, and either `--config` or `--flowcell` and `--kit`. This will generate a PBS script for a single job that distributes the basecalling calculation over a set of nodes so that the job will complete in under 96h (the maximum walltime for Mesabi's default queues.) The jobs are by default set up to complete in approximately 48h, giving a factor of 2 safety margin to allow for fluctuations in runtime.

### Example usage:

```
$ prepare_guppy.py --input_path /scratch.global/dunn0404/guppy_job_dev/fast5 --save_path output --stage_path staging --kit SQK-LSK109 --flowcell FLO-MIN106
Empirical efficiency: 0.83 h/GB
1 jobs will be submitted over a total of 5 subsets
Job 0: 5 nodes, 24 ppn
  - Set 0: 261 files (46.36 GB)
  - Set 1: 262 files (46.51 GB)
  - Set 2: 261 files (46.36 GB)
  - Set 3: 262 files (46.47 GB)
  - Set 4: 262 files (46.51 GB)
To submit all jobs, run the command 'sh submit.sh'
```

This will process the data in the directory /scratch.global/dunn0404/guppy_job_dev/fast5 into a directory named output in the current working directory. A staging directory named staging will also be created in the current working directory. The summary output indicates how the work has been partitioned. There is a single job that will process 5 subsets of the data, each subset using its own node. This script generates a single script submit.sh that you can run to submit the listed job(s).

## Advanced Settings

For some datasets, this may generate a job file that requests more nodes than you would like, leading to potentially long queueing times. In these cases, you can specify the `--njobs` flag to break the calculation down into that many jobs. The script will distribute the work between these jobs roughly equally.

### Example usage:

```
$ prepare_guppy.py --input_path /scratch.global/dunn0404/guppy_job_dev/fast5 --save_path output --stage_path staging --kit SQK-LSK109 --flowcell FLO-MIN106 --njobs 5
Empirical efficiency: 0.83 h/GB
5 jobs will be submitted over a total of 5 subsets
Job 0: 1 nodes, 24 ppn
  - Set 0: 261 files (46.36 GB)
Job 1: 1 nodes, 24 ppn
  - Set 1: 262 files (46.51 GB)
Job 2: 1 nodes, 24 ppn
  - Set 2: 261 files (46.36 GB)
Job 3: 1 nodes, 24 ppn
  - Set 3: 262 files (46.47 GB)
Job 4: 1 nodes, 24 ppn
  - Set 4: 262 files (46.51 GB)
To submit all jobs, run the command 'sh submit.sh'
```

Some datasets may have files with a wide distribution of file sizes, or a small number of files. In these cases, the partitioning strategy employed by this script can provide uneven partitioning of work between jobs. In these cases, you may wish to tune the `--nsets` parameter to manually choose how many sets to break the calculation into. You can combine this with the `--njobs` flag to tune the number of sets per job. Note that the script may select a different number of jobs if your proposed njobs would leave one or more jobs without any work to do.

### Example usage:

```
$ prepare_guppy.py --input_path /scratch.global/dunn0404/guppy_job_dev/fast5 --save_path output --stage_path staging --kit SQK-LSK109 --flowcell FLO-MIN106 --nsets 10
Empirical efficiency: 0.83 h/GB
1 jobs will be submitted over a total of 10 subsets
Job 0: 10 nodes, 24 ppn
  - Set 0: 131 files (23.25 GB)
  - Set 1: 131 files (23.26 GB)
  - Set 2: 131 files (23.21 GB)
  - Set 3: 131 files (23.26 GB)
  - Set 4: 131 files (23.26 GB)
  - Set 5: 131 files (23.26 GB)
  - Set 6: 131 files (23.26 GB)
  - Set 7: 130 files (23.10 GB)
  - Set 8: 130 files (23.10 GB)
  - Set 9: 131 files (23.25 GB)
To submit all jobs, run the command 'sh submit.sh'
```

### Example usage:

```
$ prepare_guppy.py --input_path /scratch.global/dunn0404/guppy_job_dev/fast5 --save_path test/output/data --stage_path staging --kit SQK-LSK109 --flowcell FLO-MIN106 --nsets 10 --njobs 5
Empirical efficiency: 0.83 h/GB
5 jobs will be submitted over a total of 10 subsets
Job 0: 2 nodes, 24 ppn
  - Set 0: 131 files (23.25 GB)
  - Set 1: 131 files (23.26 GB)
Job 1: 2 nodes, 24 ppn
  - Set 2: 131 files (23.21 GB)
  - Set 3: 131 files (23.26 GB)
Job 2: 2 nodes, 24 ppn
  - Set 4: 131 files (23.26 GB)
  - Set 5: 131 files (23.26 GB)
Job 3: 2 nodes, 24 ppn
  - Set 6: 131 files (23.26 GB)
  - Set 7: 130 files (23.10 GB)
Job 4: 2 nodes, 24 ppn
  - Set 8: 130 files (23.10 GB)
  - Set 9: 131 files (23.25 GB)
To submit all jobs, run the command 'sh submit.sh'
```

This script was written with the default queues on Mesabi in mind. If you are using this script elsewhere, you may wish to change the processing efficiency or the target runtime of your jobs. The arguments `--pergb` and `--walltime` provide the ability to set these parameters for your jobs. These values should be based on benchmarking a small basecalling job to determine the performance under the desired environment

