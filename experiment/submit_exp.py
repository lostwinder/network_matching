#!/usr/bin/python
import os
from random import shuffle
from subprocess import call
from time import sleep

def generate_condor_submit_file(index, file_size, need_runtime, alpha):
  filename = "condor_script_" + str(index)
  condor_script = open(filename, "w")
  condor_script.write("Universe = vanilla\n")
  if need_runtime == "N":
    condor_script.write("Executable = /bin/pwd\n")
  elif need_runtime == "Y":
    condor_script.write("Executable = /bin/sleep\n")
    condor_script.write("Arguments = " + str(alpha*file_size) + "\n")
  condor_script.write("Output = test.out."+str(index)+"\n")
  condor_script.write("Error = test.err."+str(index)+"\n")
  condor_script.write("Log = test.log."+str(index)+"\n")
  condor_script.write("input = "+str(file_size)+"MB.zip\n")
  condor_script.write("should_transfer_files = if_needed\n")
  condor_script.write("when_to_transfer_output = on_exit\n")
  condor_script.write("Queue")



def main():
  
  # initialize some variables
  # file_size:      store the list of unique input file sizes in ascending order
  # job_histogram:  store the corresponding histogram for the input file sizes
  # job_list:       the actual job list in submission order (input file size)
  # index_list:     the index for the shuffled job list
  # num_jobs:       the total number of jobs for this experiment

  file_size = []
  job_histogram = []
  job_list = []
  index_list = []
  num_jobs = 0


  need_runtime = "N"
  alpha = 1 # multiplier for runtime for jobs in terms of their input size
  run_no = int(raw_input("Index for this set of experiments: "))
  need_submit_file = raw_input("Need generating submission files? (Y or N): ")
  if need_submit_file == "Y":
    # the raw input should be separated by space
    file_size_str = raw_input("Unique file sizes in ascending order: ")
    job_histogram_str = raw_input("Corresponding job histogram: ")
    need_runtime = raw_input("Need jobs with actual run time? (Y or N): ")

    # let assume the worker node on testbed can run the jobs with length equal
    # to TransferInputSizeMB seconds by default
    if need_runtime == "Y":
      alpha_str = raw_input("Multiplier for runtime in term of input size: ")
      alpha = int(alpha_str)

    file_size = [int(i) for i in file_size_str.split(" ")]
    job_histogram = [int(i) for i in job_histogram_str.split(" ")]

    for i in range(len(file_size)):
      for j in range(job_histogram[i]):
        job_list.append(file_size[i])

    num_jobs = len(job_list)
    index_list = range(num_jobs)
    shuffle(index_list)

  elif need_submit_file == 'N':
    # figure out the number of jobs based on the number of submission scripts
    files = os.listdir(".")
    for file in files:
      if file.startswith("condor_script"):
        num_jobs += 1

  # submit 5 jobs per second
  for i in range(num_jobs):
    if (i+1)%5 == 1:
      sleep(1)
    if need_submit_file == "Y":
      if need_runtime == "Y":
        generate_condor_submit_file(i+1, job_list[index_list[i]], "Y", alpha)
      else:
        generate_condor_submit_file(i+1, job_list[index_list[i]], "N", alpha)
    call(["condor_submit", "condor_script_"+str(i+1)])

if __name__ == "__main__":
  main()
