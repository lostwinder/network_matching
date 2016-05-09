#!/usr/bin/python
from random import shuffle
from subprocess import call
from time import sleep

def generate_condor_submit_file(index, file_size):
  filename = "condor_script_" + str(index)
  condor_script = open(filename, "w")
  condor_script.write("Universe = vanilla\n")
  condor_script.write("Executable = /bin/pwd\n")
  condor_script.write("Output = test.out."+str(index)+"\n")
  condor_script.write("Error = test.err."+str(index)+"\n")
  condor_script.write("Log = test.log."+str(index)+"\n")
  condor_script.write("input = "+str(file_size)+"MB.zip\n")
  condor_script.write("should_transfer_files = if_needed\n")
  condor_script.write("when_to_transfer_output = on_exit\n")
  condor_script.write("Queue")

file_size = [5,10,20,30,40,50,60,70,80,90,100,200,300,400,500,600,700,800,900,1000]
job_histogram = [200,190,180,170,160,150,140,130,120,110,100,90,80,70,60,50,40,30,20,10]
for i in range(len(file_size)):
  job_histogram[i] = job_histogram[i]

job_list = []

for i in range(len(file_size)):
  for j in range(job_histogram[i]):
    job_list.append(file_size[i])

index_list = range(len(job_list))
shuffle(index_list)
shuffle_list = open("index_list.txt", "w")
shuffle_list.write(str(index_list))


# submit 5 jobs per second
for i in range(len(job_list)):
  if (i+1)%5 == 1:
    sleep(1)
  generate_condor_submit_file(i+1, job_list[index_list[i]])
  call(["condor_submit", "condor_script_"+str(i+1)])
