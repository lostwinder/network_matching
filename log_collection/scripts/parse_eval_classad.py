#!/usr/bin/python
# Given a list of condor history log files, this script parse each log file
# into classads and evaluate all the attributes inside each classad. The 
# purpose is to make sure that for a certain attribute, it will be always 
# evaluted to be the right format (numerical or boolean), if it is evaluated 
# to be Undefined, remove this attribute from its belonging classad

import subprocess
import classad
import types

def classad_attr_atoi(ad, attr):
  """
  convert numeric classad attribute from string to long
  """
  if attr in ad.keys():
    if ad[attr] == "":
      del ad[attr]
    else:
      if type(ad[attr]) is types.StringType:
        ad[attr] = long(ad[attr])

# Get the list of condor history log files
dir = "/home/bockelman/zzhang/ELK_stack/condor_history_log_backup_new"
p = subprocess.Popen("ls " + dir, stdout=subprocess.PIPE, shell=True)
(output, err) = p.communicate()

p_status = p.wait()

lines = output.split('\n')
# remove the empty lines
lines = filter(lambda a: a != '', lines)

# iterate and process each condor history log file
for line in lines:
  input = open(dir+"/"+line, "r")
  output = open(dir+"/p_"+line, "a+")
  global_job_id_set = set()
  while True:
    try:
      ad = classad.parseNext(input)
      for k in ad:
        if ad.eval(k) is classad.Value.Undefined:
          del ad[k]
        else:
          ad[k] = ad.eval(k)
      if "GlobalJobId" in ad.keys():
        if ad["GlobalJobId"] in global_job_id_set:
          pass
        else:
          global_job_id_set.add(ad["GlobalJobId"])
          classad_attr_atoi(ad, "Chirp_StashCp_DlTimeMs")
          classad_attr_atoi(ad, "Chirp_StashCp_FileSize")
          output.write(ad.printOld()+"\n")
      else:
        classad_attr_atoi(ad, "Chirp_StashCp_DlTimeMs")
        classad_attr_atoi(ad, "Chirp_StashCp_FileSize")
        output.write(ad.printOld()+"\n")
    except StopIteration:
      break

  input.close()
  output.close()
