#!/usr/bin/env python3

import os
import glob
import argparse
import math

# function definitions
def smallest_subset(subsets):
	"""Returns the index of the smallest subset by summation
	of the elements in each subset
	"""

	min_set = sum(subsets[0])
	jj_min_set = 0
	for ii in range(0, len(subsets)):
		set_sum = sum(subsets[ii])
		if set_sum == 0:
			return ii
		elif set_sum < min_set:
			jj_min_set = ii
			min_set = set_sum

	return jj_min_set


def find_partition(sizes, files, N):
	"""Separate files into N subsets of approximately equal size
	using a greedy partitioning algorithm. This breaks down for
	sets of files with vastly different sizes.
	"""

	file_subsets = [ [] for ii in range(0, N) ]
	size_subsets = [ [] for ii in range(0, N) ]
	final_sizes  = [ 0  for ii in range(0, N) ]

	sizes, files = zip(*sorted(zip(sizes, files), reverse=True))
	sizes = list(sizes)
	files = list(files)

	for ii in range(0, len(files)):
		jj = smallest_subset(size_subsets)
		file_subsets[jj].append(files[ii])
		size_subsets[jj].append(sizes[ii])
		final_sizes[jj] += sizes[ii]

	if sum(final_sizes) != sum(sizes):
		print("ERROR IN PARTITIONING")

	final_sizes = [total / 1024 / 1024 / 1024 for total in final_sizes]
	formatted = ["%0.2f" % total for total in final_sizes]	
	print("Proposed sizes: {0} GB".format(formatted))

	return size_subsets, file_subsets


# parse user input
parser = argparse.ArgumentParser(description="Submit batches of guppy jobs to PBS scheduler")

required = parser.add_argument_group('required arguments')
optional = parser.add_argument_group('optional arguments')

required.add_argument('--input_path', type=str, 
	help='directory containing files to process', required=True)
required.add_argument('--save_path', type=str,
	help='directory to store output files', required=True)
required.add_argument('--stage_path', type=str,
	help='directory to stage calculations', required=True)
optional.add_argument('--flowcell', type=str, help='flowcell version')
optional.add_argument('--kit', type=str, help='sequencing kit version')
optional.add_argument('--config', type=str, help='config file with guppy parameters')
optional.add_argument('--nsets', type=int, help='break calculation into this many subsets (default: determine from file sizes)')
optional.add_argument('--njobs', type=int, 
	help='break calculation into this many PBS jobs (default: 1)', default=1)
optional.add_argument('--ppn', type=int, help='processors per node to request for jobs (Default: 24)', default=24)
optional.add_argument('--pergb', type=float, help='CPU-hours to process 1 GB of files (default: 20, based on a 24 core Mesabi job )', default=20.0)
optional.add_argument('--walltime', type=float, help='Target real walltime that each job should run. Jobs will request double this time as a buffer. (Default: 48)', default=48.0)

args = parser.parse_args()

# we need either a flowcell and a kit, or a config file to proceed
# and we can't have both
if not args.config and (not (args.flowcell and args.kit)):
	parser.error('you must specify --flowcell and --kit, or specify a file with --config')
elif args.config and (args.flowcell and args.kit):
	parser.error('you must only specify either --flowcell and --kit, or a file with --config')

# our input directory must exist, and we need to create the output
# and staging directories if they don't exist
input_path = args.input_path
save_path = args.save_path
stage_path = args.stage_path

if not os.path.isdir(input_path):
	raise OSError('Directory \'{0}\' does not exist'.format(input_path))

if not os.path.isdir(save_path):
	os.makedirs(save_path)
if not os.path.isdir(stage_path):
	os.makedirs(stage_path)

if args.flowcell and args.kit:
	flowcell = args.flowcell
	kit = args.kit
else:
	config = args.config

# recursively get list of files from input directory, then their sizes
files = []
sizes = []
total_size = 0.0
for filename in glob.iglob(input_path + '**/*', recursive=True):
	files.append(filename)

	file_size = os.path.getsize(filename)
	sizes.append(file_size)
	total_size += file_size
	

# parse out approximately equal file sizes into subsets (possibly tough)
# - target a default runtime of 48h
# - put a limit on the number of subsets unless a flag is passed
sizes, files = zip(*sorted(zip(sizes, files)))
sizes = list(sizes)
files = list(files)

empirical_efficiency = args.pergb / args.ppn 
chunk_size = args.walltime / empirical_efficiency
chunk_size_bytes = chunk_size * 1024 * 1024 * 1024

if args.nsets:
	nsets = args.nsets
else:
	nsets = math.ceil(total_size / chunk_size_bytes)

if args.njobs > nsets:
	njobs = nsets
	print("WARNING: The number of subsets is less than the number of jobs requested.\n The number of jobs has been set to the number of subsets")
else:
	njobs = args.njobs

print("Empirical efficiency: {0:.2f} h/GB".format(empirical_efficiency))
print("Empirical max subset size: {0:.2f} GB".format(chunk_size)) 
print("Will use {0} subsets in {1} job(s)".format(nsets, njobs))

if max(sizes) > chunk_size_bytes:
	print("ERROR: Estimated chunk size {0} is smaller than largest file {1}", 
		chunk_size_bytes, max(sizes))
	exit(1)

size_subsets, file_subsets = find_partition(sizes, files, nsets)

# make symlinks to these subsets in staging directory
for ii in range(0, nsets):
	subdirname = "{0}/subset{1}".format(stage_path, ii)
	os.makedirs(subdirname)
	
	for filepath in file_subsets[ii]:
		filename = filepath.split('/')[-1]
		dest = "{0}/{1}".format(subdirname, filename)
		os.symlink(filepath, dest)


# create job files matching requested profile


pbs_preamble = ""
pbs_preamble += "#!/bin/bash -l\n"
pbs_preamble += "#PBS -l nodes={0}:ppn={1},walltime=96:00:00\n"
pbs_preamble += "$PBS -m abe\n"
pbs_preamble += "$PBS -j oe\n"
pbs_preamble += "$PBS -N {2}\n"
pbs_preamble += "\n"

if njobs == nsets:
	# loop over sets and fill in pbs contents for each
	for ii in range(0, nsets):
		subdirname = "{0}/subset{1}".format(stage_path, ii)

		pbs_contents = pbs_preamble.format(1, args.ppn, "guppy_set{0}".format(ii))
		pbs_contents += "module load guppy\n"
		pbs_contents += "<insert guppy command here>\n"
		
		pbs_file = open("{0}/subset{1}.pbs".format(stage_path, ii), "w")
		pbs_file.write(pbs_contents)
		pbs_file.close()

elif njobs < nsets:
	# we need to create a GNU parallel strategy for this
	sets_per_job = math.ceil(float(nsets)/njobs)

	set_index = 0
	for ii in range(0, njobs):

		proposed_max = sets_per_job + set_index
		if proposed_max > nsets:
			max_index = nsets
			sets_per_job = nsets % sets_per_job
		else:
			max_index = proposed_max
			

		cmd_contents = ""

		for jj in range(set_index, max_index):
			cmd_contents += "<insert guppy command {0} here>\n".format(jj)

		cmd_filename = "{0}/job{1}.cmd".format(stage_path, ii) 
		cmd_file = open(cmd_filename, "w")
		cmd_file.write(cmd_contents)
		cmd_file.close()
 
		nodelist = "nodelist_job{0}.txt".format(ii)

		pbs_contents = pbs_preamble.format(sets_per_job, args.ppn, 
			"guppy_sets{0}.{1}".format(set_index, max_index-1))

		pbs_contents += "module load parallel\n"
		pbs_contents += "module load guppy\n"
		pbs_contents += "sort -u $PBS_NODEFILE > {0}\n".format(nodelist)
		pbs_contents += "export PARALLEL=\"--workdir . --env PATH --env LD_LIBRARY_PATH --env LOADEDMODULES --env _LMFILES_ --env MODULE_VERSION --env MODULEPATH --env MODULEVERSION_STACK --env MODULESHOME --env OMP_DYNAMICS --env OMP_MAX_ACTIVE_LEVELS --env OMP_NESTED --env OMP_NUM_THREADS --env OMP_SCHEDULE --env OMP_STACKSIZE --env OMP_THREAD_LIMIT --env OMP_WAIT_POLICY\"\n"
		pbs_contents += "parallel --jobs {0} --sshloginfile {1} --workdir $PWD < {2}\n".format(1, nodelist, cmd_filename)


		pbs_file= open("{0}/job{1}.pbs".format(stage_path, ii), "w")
		pbs_file.write(pbs_contents)
		pbs_file.close()

		set_index = max_index

# report summary statistics

# produce script to submit jobs
