#!/usr/bin/python
import os
import subprocess

num = int(raw_input("Enter the number for condor_history '-match' argument:"))
job_ad_file = raw_input("Enter the filename for saving complete condor job classads:")

cwd = os.getcwd()
if os.path.exists(cwd + "/" + job_ad_file):
  pass
else:
  f = open(job_ad_file, "w")
  subprocess.call(["condor_history", "-match", str(num), "-long"], stdout=f)
  f.close()

qdate = []
start_date = []
start_exec_date = []
completion_date = []

f = open(job_ad_file, "r")
lines = f.readlines()
for line in lines:
  if "QDate" in line:
    qdate.append(line.split(" ")[2][:-1])
  if "JobCurrentStartDate" in line:
    start_date.append(line.split(" ")[2][:-1])
  if "JobCurrentStartExecutingDate" in line:
    start_exec_date.append(line.split(" ")[2][:-1])
  if "CompletionDate" in line:
    completion_date.append(line.split(" ")[2][:-1])


total_file_transfer_time = 0
total_job_time = 0

for i in range(num):
  total_file_transfer_time += long(start_exec_date[i]) - long(start_date[i])
  total_job_time += long(completion_date[i]) - long(qdate[i])

print 'Total file transfer time is %ld.' % total_file_transfer_time
print 'Total job time is %ld.' % total_job_time
