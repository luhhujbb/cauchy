(ns cauchy.jobs.hbase
  (:require [clj-http.client :as http]))

(def master-last-req-count (atom 0))
(def region-last-req-count (atom 0))
(def last-w-req-count (atom 0))
(def last-r-req-count (atom 0))


(defn fetch-metrics
  "Generic method to fetch jmx metrics"
  [{:keys [host port]}]
    (let [url (str "http://" host ":" port "/jmx")]
      (:body
        (try
          (http/get url {:as :json})
          (catch Exception e
            {:body {:error true}})))))

(defn filter-stats
  "A retrieve a specific bean from a jmx json"
	[stats mygroup]
		(first (filter #(= (:name %) mygroup) (:beans stats))))

(defn per-sec
  [atom-to-update new-value period]
  (let [res (if-not (= 0 period)
        (/ (- new-value (deref atom-to-update)) period)
        0)]
      (reset! atom-to-update new-value)
      res))

(defn master-cluster-state
  [input period]
  (let [req-per-sec (per-sec master-last-req-count (:clusterRequests input) period)]
    (if (= (:tag.isActiveMaster input) "true")
      [{:service "master" :metric 1}
       {:service "cluster.live_regionservers" :metric (:numRegionServers input)}
       {:service "cluster.dead_regionservers" :metric (:numDeadRegionServers input)}
       {:service "req.total_requests" :metric (:clusterRequests input)}
       {:service "req.total_request_sec" :metric req-per-sec}
       {:service "cluster.region_load" :metric (:averageLoad input)}]
      [{:service "master" :metric 0}])))

(defn jvm-state
  [input period]
  [{:service "mem.non_heap_max" :metric (:MemNonHeapMaxM input)}
   {:service "mem.non_heap_used" :metric (:MemNonHeapUsedM input)}
   {:service "mem.heap_max" :metric (:MemMaxM input)}
   {:service "mem.heap_used" :metric (:MemHeapUsedM input)}
   {:service "gc.count" :metric (:GcCount input)}
   {:service "gc.total_time" :metric (:GcTimeMillis input)}
   {:service "gc.avg_time" :metric (if-not (= 0 (:GcCount input)) (/ (:GcTimeMillis input) (:GcCount input)) 0)}])

(defn master
  ([{:keys [host port period] :or {host "localhost" port 16010} :as conf}]
  (let [metrics (fetch-metrics conf)]
    (if-not (:error metrics)
        (let [input-master (filter-stats metrics "Hadoop:service=HBase,name=Master,sub=Server")
              input-jvm (filter-stats metrics "Hadoop:service=HBase,name=JvmMetrics")]
          (into [] (concat (master-cluster-state input-master period) (jvm-state input-jvm period))))
          [])))
  ([] master {}))


(defn regionserver-state
  [input period]
  (let [req-per-sec (per-sec region-last-req-count (:totalRequestCount input) period)
        req-w-per-sec (per-sec last-w-req-count (:writeRequestCount input) period)
        req-r-per-sec (per-sec last-r-req-count (:readRequestCount input) period)]
  [{:service "request_sec" :metric req-per-sec}
   {:service "read_request_sec" :metric req-r-per-sec}
   {:service "write_request_sec" :metric req-w-per-sec}
   {:service "regionCount" :metric (:regionCount input)}
   {:service "req.time.mutate_avg" :metric (:Mutate_mean input)}
   {:service "req.time.scan_next_avg" :metric (:ScanNext_mean input)}
   {:service "req.time.flush" :metric (:FlushTime_mean input)}
   {:service "req.time.get" :metric (:Get_mean input)}
   {:service "req.time.inc" :metric (:Increment_mean input)}
   {:service "req.slow.get" :metric (:slowGetCount input)}
   {:service "req.slow.append" :metric (:slowAppendCount input)}
   {:service "req.slow.put" :metric (:slowPutCount input)}
   {:service "queue.split.length" :metric (:splitQueueLength input)}
   {:service "queue.compaction.lenght" :metric (:compactionQueueLength input)}
   {:service "queue.flush.length" :metric (:flushQueueLength input)}
   {:service "req.blocked.count" :metric (:blockedRequestCount input)}
   {:service "percent_files_local" :metric (:percentFilesLocal input)}
   {:service "store.count" :metric (:storeCount input)}
   {:service "store.file.size" :metric (:storeCount input)}
   {:service "store.file.index_size" :metric (:storeFileIndexSize input)}
   {:service "store.mem.size" :metric (:memStoreSize input)}
   {:service "store.hlog_file.size" :metric (:hlogFileSize input)}
   {:service "store.hlog_file.count" :metric (:hlogFileCount input)}
   {:service "cells.compacted.count" :metric (:compactedCellsCount input)}
   {:service "cells.compacted.size" :metric (:compactedCellsSize input)}
   {:service "cells.major_compacted.count" :metric (:majorCompactedCellsCount input)}
   {:service "cells.major_compacted.size" :metric (:majorCompactedCellsSize input)}
   ]))

(defn operating-system-state
  [input period]
  [{:service "open_files.max" :metric (:MaxFileDescriptorCount input)}
   {:service "open_files.count" :metric (:OpenFileDescriptorCount input)}])

(defn threading-state
  [input period]
  [{:service "thread.total_started" :metric (:TotalStartedThreadCount input)}
   {:service "thread.count" :metric (:ThreadCount input)}
   {:service "thread.peak_count" :metric (:PeakThreadCount input)}])

(defn regionserver
  ([{:keys [host port period] :or {host "localhost" port 16010} :as conf}]
  (let [metrics (fetch-metrics conf)]
    (if-not (:error metrics)
        (let [input-regionserver (filter-stats metrics "Hadoop:service=HBase,name=RegionServer,sub=Server")
              input-jvm (filter-stats metrics "Hadoop:service=HBase,name=JvmMetrics")
              input-os (filter-stats metrics "java.lang:type=OperatingSystem")
              input-threading (filter-stats metrics "java.lang:type=Threading")]
          (into [] (concat (regionserver-state input-regionserver period)
                            (jvm-state input-jvm period)
                            (operating-system-state input-os period)
                            (threading-state input-threading period))))
          [])))
  ([] regionserver {}))
