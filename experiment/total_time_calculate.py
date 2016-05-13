#!/usr/bin/python
import os
import subprocess

num = int(raw_input("Enter the number for condor_history '-match' argument:"))
job_ad_file = raw_input("Enter the filename for saving complete condor job classads:")
job_start_date_file = raw_input("Enter the filename for saving the JobCurrentStartDate attribute values:")
job_start_exec_date_file = raw_input("Enter the filename for saving the JobCurrentStartExecutingDate attribute values:")

cwd = os.getcwd()
print cwd + "/" + job_ad_file
if os.path.exists(cwd + "/" + job_ad_file):
  pass
else:
  f = open(job_ad_file, "w")
  subprocess.call(["condor_history", "-match", str(num), "-long"], stdout=f)
  f.close()

if os.path.exists(cwd + "/" + job_start_date_file):
  pass
else:
  f1 = open(job_ad_file, "r")
  f2 = open(job_start_date_file, "w")
  f3 = open(job_start_exec_date_file, "w")
  lines = f1.readlines()
  for line in lines:
    if "JobCurrentStartDate" in line:
      f2.write(line)
    if "JobCurrentStartExecutingDate" in line:
      f3.write(line)
  f1.close()
  f2.close()
  f3.close()

exp_start_date = open(job_start_date_file, "r")
lines = exp_start_date.readlines()
exp_start_date_list = []
for line in lines:
  exp_start_date_list.append(line.split(" ")[2][:-1])

exp_start_exec_date = open(job_start_exec_date_file, "r")
lines = exp_start_exec_date.readlines()
exp_start_exec_date_list = []
for line in lines:
  exp_start_exec_date_list.append(line.split(" ")[2][:-1])

total_time = 0

for i in range(len(exp_start_date_list)):
  total_time = long(exp_start_exec_date_list[i]) - long(exp_start_date_list[i]) + total_time

print total_time
