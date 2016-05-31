#!/usr/bin/python
import os
import subprocess

num = int(raw_input("Number for condor_history '-match' argument: "))
job_ad_file = raw_input("Filename for saving complete condor job classads: ")
exp_result_file = raw_input("Filename for saving the experiment results: ")

cwd = os.getcwd()
if os.path.exists(cwd + "/" + job_ad_file):
  # sometimes we just want to recalculate the time statistics  
  # on some exisiting condor_history file, therefore no need to 
  # run condor_history command and save to the same file again
  pass
else:
  f = open(job_ad_file, "w")
  subprocess.call(["condor_history", "-match", str(num), "-long"], stdout=f)
  f.close()

cluster_id = []
execution_site = []
transfer_input_size = []
qdate = []
start_date = []
start_exec_date = []
completion_date = []

# key: ClusterId
# value: a list of attributes, the order of attributes are as following
# 1:ExecutionSite         2:TransferInputSizeMB          3:QDate 
# 4:JobCurrentStartDate   5:JobCurrentStartExecutingDate 6:JobCompletionDate 
# 7:RelativeSubmitDate    8:QueuedTime                   9:ActualTransferTime
# 10:ExecutionTime        11:EstimatedTransferTime       12:MachineSlot
time_stat_dict = {}

f = open(job_ad_file, "r")
lines = f.readlines()
for line in lines:
  if "ClusterId" in line:
    cluster_id.append(line.split(" ")[2][:-1])
  if "MATCH_EXP_Glidein_CMSSite" in line:
    execution_site.append(line.split(" ")[2][:-1])
  if "TransferInputSizeMB" in line:
    transfer_input_size.append(line.split(" ")[2][:-1])
  if "QDate" in line:
    qdate.append(line.split(" ")[2][:-1])
  if "JobCurrentStartDate" in line:
    start_date.append(line.split(" ")[2][:-1])
  if "JobCurrentStartExecutingDate" in line:
    start_exec_date.append(line.split(" ")[2][:-1])
  if "CompletionDate" in line:
    completion_date.append(line.split(" ")[2][:-1])

# get the earliest submit time
earliest_submit_time = long(qdate[0])
for value in qdate:
  if earliest_submit_time > long(value):
    earliest_submit_time = long(value)
relative_submit_time = []
for value in qdate:
  relative_submit_time.append(str(long(value)-earliest_submit_time))
queued_time = []
execution_time = []
for i in range(num):
  queued_time.append(str(long(start_date[i])-long(qdate[i])))
  execution_time.append(str(long(completion_date[i])-long(start_exec_date[i])))


for i in range(num):
  key = cluster_id[i]
  time_stat_dict[key] = []
  time_stat_dict[key].append(execution_site[i])
  time_stat_dict[key].append(transfer_input_size[i])
  time_stat_dict[key].append(qdate[i])
  time_stat_dict[key].append(start_date[i])
  time_stat_dict[key].append(start_exec_date[i])
  time_stat_dict[key].append(completion_date[i])
  time_stat_dict[key].append(relative_submit_time[i])
  time_stat_dict[key].append(queued_time[i])
  actual_transfer_time = long(start_exec_date[i]) - long(start_date[i])
  time_stat_dict[key].append(str(actual_transfer_time))
  time_stat_dict[key].append(execution_time[i])
    

total_file_transfer_time = 0
total_job_time = 0

for i in range(num):
  total_file_transfer_time += long(start_exec_date[i]) - long(start_date[i])
  total_job_time += long(completion_date[i]) - long(qdate[i])

print 'Total file transfer time is %ld.' % total_file_transfer_time
print 'Total job time is %ld.' % total_job_time

# process condor negotiator log for estimated file tranfer time in  matchmaking
# first process NegotiatorLog.old, then NegotiatorLog
negotiator_log = "/var/log/condor/NegotiatorLog"
negotiator_log_old = "/var/log/condor/NegotiatorLog.old"
grep_output = str()

if os.path.exists(negotiator_log_old):
  grep_output += subprocess.Popen(['grep', 'Exp:', negotiator_log_old], 
                                    stdout=subprocess.PIPE).communicate()[0]
if os.path.exists(negotiator_log):
  grep_output += subprocess.Popen(['grep', 'Exp:', negotiator_log], 
                                    stdout=subprocess.PIPE).communicate()[0]

grep_output = grep_output.split('\n')
# remove the last empty string
grep_output = grep_output[:-1]
# only use the last num_of_jobs entries of the grep output
estimation_info = grep_output[-num:]
for line in estimation_info:
  columns = line.split(",")
  cluster_id = columns[0].split(" ")[-1]
  estimated_transfer_time = columns[2].split(" ")[-1]
  execution_slot = columns[3].split(" ")[-1]
  time_stat_dict[cluster_id].append(estimated_transfer_time)
  time_stat_dict[cluster_id].append(execution_slot)


# write the actual and estimated file transfer information to CSV file
# the columns are arranged in the following structure
# 1:ClusterId          2:ExecutionSite          3:TransferInputSizeMB
# 4:QDate              5:JobCurrentStartDate    6:JobCurrentStartExecutingDate 
# 7:JobCompletionDate  8:RelativeSubmitDate     8:QueuedTime             
# 9:ActualTransferTime 10:ExecutionTime         11:EstimatedTransferTime 
# 12:MachineSlot       13:MachineSlotRenamed    

f = open(exp_result_file, 'w')
f.write("ClusterId" + " " + "ExecutionSite" + " " + "TransferInputSizeMB" + " "
        + "QDate" + " " + "JobCurrentStartDate" + " " + 
	"JobCurrentStartExecutingDate" + " " + "JobCompletionDate" + " " +
	"SubmitTime" + " " + "ActualTransferTime" + " " + "QueuedTime" + " " 
	+ "ExecTime" + "EstimatedTransferTime" + " " + 
	"MachineSlot" + " " + "MachineSlotRenamed" + "\n")
for key in sorted(time_stat_dict):
  line = key
  for attr in time_stat_dict[key]:
    line += " "
    line += attr
  # MachineSlotRenamed (This is for sorting in excel based on this attr)
  slot_name = time_stat_dict[key][-1]
  words = slot_name.split("@")
  renamed_slot = words[1] + "@" + words[0]
  line += " "
  line += renamed_slot
  f.write(line + '\n')

f.write("Total_file_transfer_time: " + str(total_file_transfer_time) + "\n")
f.write("Total_job_time: " + str(total_job_time) + "\n")
