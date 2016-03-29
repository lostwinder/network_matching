#!/usr/bin/python
import htcondor
import classad
import time
import datetime
import elasticsearch
import json

collector_addr = "osg-flock.grid.iu.edu"
coll = htcondor.Collector(collector_addr)

projected_attr = ["Machine", "Name"]
schedd_list = coll.query(htcondor.AdTypes.Schedd, projection=projected_attr)

string_values = set([\
  "AddressV1",
  "AuthenticatedIdentity",
  "CollectorHost",
  "CondorPlatform",
  "CondorVersion",
  "JobsBadputRuntimes",
  "JobsBadputSizes",
  "JobsCompletedRuntimes",
  "JobsCompletedSizes",
  "JobsRestartReconnectsBadput",
  "JobsRunningRuntimes",
  "JobsRunningSizes",
  "JobsRuntimesHistogramBuckets",
  "JobsSizesHistogramBuckets",
  "Machine",
  "MyAddress",
  "MyType",
  "Name",
  "RecentJobsBadputRuntimes",
  "RecentJobsBadputSizes",
  "RecentJobsCompletedRuntimes",
  "RecentJobsCompletedSizes",
  "ScheddIpAddr",
  "ScheddName",
  "StartLocalUniverse",
  "StartSchedulerUniverse",
  "TargetType",
  "TransferQueueUserExpr",
  "UpdatesHistory",
])

int_values = set([\
  "Autoclusters",
  "JobsAccumRunningTime",
  "RecentShadowsStarted",
  "DetectedCpus",
  "DetectedMemory",
  "FileTransferDownloadBytes",
  "FileTransferFileReadSeconds",
  "FileTransferFileWriteSeconds",
  "FileTransferMBWaitingToDownload",
  "FileTransferMBWaitingToUpload",
  "FileTransferNetReadSeconds",
  "FileTransferNetWriteSeconds",
  "FileTransferUploadBytes",
  "JobsAccumBadputTime",
  "JobsAccumChurnTime",
  "JobsAccumExceptionalBadputTime",
  "JobsAccumExecuteAltTime",
  "JobsAccumExecuteTime",
  "JobsAccumPostExecuteTime",
  "JobsAccumPreExecuteTime",
  "JobsAccumRunningTime",
  "JobsAccumTimeToStart",
  "JobsCheckpointed",
  "JobsCompleted",
  "JobsCoredumped",
  "JobsDebugLogError",
  "JobsExecFailed",
  "JobsExitException",
  "JobsExited",
  "JobsExitedAndClaimClosing",
  "JobsExitedNormally",
  "JobsKilled",
  "JobsMissedDeferralTime",
  "JobsNotStarted",
  "JobsRestartReconnectsAttempting",
  "JobsRestartReconnectsFailed",
  "JobsRestartReconnectsInterrupted",
  "JobsRestartReconnectsLeaseExpired",
  "JobsRestartReconnectsSucceeded",
  "JobsRunning",
  "JobsShadowNoMemory",
  "JobsShouldHold",
  "JobsShouldRemove",
  "JobsShouldRequeue",
  "JobsStarted",
  "JobsSubmitted",
  "JobsWierdTimestamps",
  "MaxJobsRunning",
  "MonitorSelfAge",
  "MonitorSelfImageSize",
  "MonitorSelfRegisteredSocketCount",
  "MonitorSelfResidentSetSize",
  "MonitorSelfSecuritySessions",
  "NumJobStartsDelayed",
  "NumPendingClaims",
  "NumUsers",
  "RecentAutoclusters",
  "RecentDaemonCoreDutyCycle",
  "RecentJobsAccumBadputTime",
  "RecentJobsAccumChurnTime",
  "RecentJobsAccumExceptionalBadputTime",
  "RecentJobsAccumExecuteAltTime",
  "RecentJobsAccumExecuteTime",
  "RecentJobsAccumPostExecuteTime",
  "RecentJobsAccumPreExecuteTime",
  "RecentJobsAccumRunningTime",
  "RecentJobsAccumTimeToStart",
  "RecentJobsCheckpointed",
  "RecentJobsCompleted",
  "RecentJobsCoredumped",
  "RecentJobsDebugLogError",
  "RecentJobsExecFailed",
  "RecentJobsExitException",
  "RecentJobsExited",
  "RecentJobsExitedAndClaimClosing",
  "RecentJobsExitedNormally",
  "RecentJobsKilled",
  "RecentJobsMissedDeferralTime",
  "RecentJobsNotStarted",
  "RecentJobsShadowNoMemory",
  "RecentJobsShouldHold",
  "RecentJobsShouldRemove",
  "RecentJobsShouldRequeue",
  "RecentJobsStarted",
  "RecentJobsSubmitted",
  "RecentJobsWierdTimestamps",
  "RecentResourceRequestsSent",
  "RecentShadowsStarted",
  "RecentStatsLifetime",
  "ResourceRequestsSent",
  "ServerTime",
  "ShadowsRunning",
  "ShadowsRunningPeak",
  "ShadowsStarted",
  "StatsLifetime",
  "TotalFlockedJobs",
  "TotalHeldJobs",
  "TotalIdleJobs",
  "TotalJobAds",
  "TotalLocalJobsIdle",
  "TotalLocalJobsRunning",
  "TotalRemovedJobs",
  "TotalRunningJobs",
  "TotalSchedulerJobsIdle",
  "TotalSchedulerJobsRunning",
  "TransferQueueDownloadWaitTime",
  "TransferQueueDownloadWaitTimePeak",
  "TransferQueueMaxDownloading",
  "TransferQueueMaxUploading",
  "TransferQueueNumDownloading",
  "TransferQueueNumDownloadingPeak",
  "TransferQueueNumUploading",
  "TransferQueueNumUploadingPeak",
  "TransferQueueNumWaitingToDownload",
  "TransferQueueNumWaitingToDownloadPeak",
  "TransferQueueNumWaitingToUpload",
  "TransferQueueNumWaitingToUploadPeak",
  "TransferQueueUploadWaitTime",
  "TransferQueueUploadWaitTimePeak",
  "UpdateSequenceNumber",
  "UpdatesLost",
  "UpdatesSequenced",
  "UpdatesTotal",
  "VirtualMemory",
])

float_values = set([\
  "DaemonCoreDutyCycle",
  "FileTransferDownloadBytesPerSecond_1d",
  "FileTransferDownloadBytesPerSecond_1h",
  "FileTransferDownloadBytesPerSecond_1m",
  "FileTransferDownloadBytesPerSecond_5m",
  "FileTransferDownloadGB",
  "FileTransferFileReadLoad_1d",
  "FileTransferFileReadLoad_1h",
  "FileTransferFileReadLoad_1m",
  "FileTransferFileReadLoad_5m",
  "FileTransferFileWriteLoad_1d",
  "FileTransferFileWriteLoad_1h",
  "FileTransferFileWriteLoad_1m",
  "FileTransferFileWriteLoad_5m",
  "FileTransferNetReadLoad_1d",
  "FileTransferNetReadLoad_1h",
  "FileTransferNetReadLoad_1m",
  "FileTransferNetReadLoad_5m",
  "FileTransferNetWriteLoad_1d",
  "FileTransferNetWriteLoad_1h",
  "FileTransferNetWriteLoad_1m",
  "FileTransferNetWriteLoad_5m",
  "FileTransferUploadBytesPerSecond_1d",
  "FileTransferUploadBytesPerSecond_1h",
  "FileTransferUploadBytesPerSecond_1m",
  "FileTransferUploadBytesPerSecond_5m",
  "FileTransferUploadGB",
  "MonitorSelfCPUUsage",
])



bool_values = set([\
  "CurbMatchmaking",
  "ScheddSwapExhausted",
  "WantResAd",
])

date_values = set([\
  "DaemonStartTime",
  "DataCollectionDate",
  "JobQueueBirthdate",
  "LastHeardFrom",
  "MonitorSelfTime",
  "MyCurrentTime",
])

file_transfer_queue_attributes = set([\
  "FileTransferDownloadBytes",
  "FileTransferUploadBytes",
  "FileTransferMBWaitingToDownload",
  "FileTransferMBWaitingToUpload",
  "TransferQueueDownloadWaitTimePeak",
  "TransferQueueUploadWaitTimePeak",
  "TransferQueueMaxDownloading",
  "TransferQueueMaxUploading",
  "TransferQueueNumDownloading",
  "TransferQueueNumUploading",
  "TransferQueueNumDownloadingPeak",
  "TransferQueueNumUploadingPeak",
  "TransferQueueNumWaitingToDownload",
  "TransferQueueNumWaitingToUpload",
  "TransferQueueNumWaitingToDownloadPeak",
  "TransferQueueNumWaitingToUploadPeak",
  "TransferQueueUploadWaitTime",
  "TransferQueueDownloadWaitTime",
])

path_prefix = "/var/log/osg_schedd_stats/"

start_time = int(time.time())


def get_all_schedd_ads():

  schedd_ads = []

  for i in range(len(schedd_list)):
    schedd_name = str()
    if schedd_list[i]["Name"] == "":
      schedd_name = schedd_list[i]["Machine"]
    else:
      schedd_name = schedd_list[i]["Name"]

    print schedd_name

    #output = open(path_prefix+schedd_name+".txt", 'a')
    #output = open(schedd_name+".txt", 'a')
    try:
      schedd_ad = coll.directQuery(htcondor.DaemonTypes.Schedd, name=schedd_name)
    except ValueError: # unable to find daemon
      continue
    except IOError: # failed to communicate with collector
      continue

    schedd_ads.append(schedd_ad)
    #for attribute in file_transfer_queue_attributes:
    #  if attribute in schedd_ad:
    #    output.write(attribute + " = " + str(schedd_ad[attribute]) + "\n")
    #output.write("\n")
  return schedd_ads

def preprocess_schedd_ads(schedd_ads):
  results = []
  for ad in schedd_ads:
      result = {}
      result["DataCollectionDate"] = start_time
      result["ScheddName"] = ad["Name"]
      result["FileTransferUploadGB"] = ad.get("FileTransferUploadBytes", 0) / 1024 / 1024 / 1024
      result["FileTransferDownloadGB"] = ad.get("FileTransferDownloadBytes", 0) / 1024 / 1024 / 1024

      for key in ad.keys():
        try:
          result[key] = ad.eval(key)
        except:
          continue

      json_ad = json.dumps(result)
      print json_ad
      datetime_object = datetime.datetime.utcfromtimestamp(start_time)
      timestamp_str = str(datetime_object).replace(' ', '-')[:-7]
      id = result["Name"] + "##" + timestamp_str
      results.append((id, json_ad))
      
  return results

def make_mappings():
  mappings = {}
  for name in int_values:
    mappings[name] = {"type": "long"}
  for name in float_values:
    mappings[name] = {"type": "double"}
  for name in string_values:
    mappings[name] = {"type": "string", "index": "not_analyzed"}
  for name in date_values:
    mappings[name] = {"type": "date", "format": "epoch_second"}
  for name in bool_values:
    mappings[name] = {"type": "boolean"}
  return mappings

_es_handle = None
def get_server_handle():
  global _es_handle
  if not _es_handle:
    _es_handle = elasticsearch.Elasticsearch()
  return _es_handle

def make_mapping(idx):
  _es_handle = get_server_handle()
  idx_clt = elasticsearch.client.IndicesClient(_es_handle)
  mappings = make_mappings()
  body = json.dumps({"mappings": {"schedd": {"properties": mappings} }})
  result = _es_handle.indices.create(index=idx, body=body, ignore=400)
  if result.get("status") != 400:
    print "Creation of index %s: %s" % (idx, str(result))

_index_cache = set()
def get_index(timestamp):
  _es_handle = get_server_handle()
  idx = time.strftime("osg-schedd-%Y-%m-%d", datetime.datetime.utcfromtimestamp(timestamp).timetuple())
  if idx in _index_cache:
    return idx
  make_mapping(idx)
  _index_cache.add(idx)
  return idx

def post_ads(es, idx, ads):
  body = ''
  for id, ad in ads:
    body += json.dumps({"index": {"_index": idx, "_type": "schedd", "_id": id}}) + "\n"
    body += ad + "\n"
  es.bulk(body=body) 

if __name__ == "__main__":
  schedd_ads = get_all_schedd_ads()
  results = preprocess_schedd_ads(schedd_ads)
  idx = get_index(start_time)
  es = htcondor_es.es.get_server_handle()
  post_ads(es, idx, results)
