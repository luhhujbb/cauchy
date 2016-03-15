(ns cauchy.jobs.hbase
  (:require [clj-http.client :as http]))

(defn fetch-stats
  [{:keys [host port ] :or {host "localhost" port 60010 }}]
    (let [url (str "http://" host ":" port "/jmx")]
        (:body (http/get url {:as :json}))))

(defn fetch-stats-filtred 
	[mygroup] 
		(filter #(= (:name %) mygroup)(:beans(fetch-stats {}))))


(let [map-epure (first(fetch-stats-filtred "java.lang:type=Threading"))
	  {:keys [ThreadCount PeakThreadCount DaemonThreadCount]} map-epure]
(print ThreadCount " ")(print PeakThreadCount " ") (print DaemonThreadCount " " ))


; donnée finale à balancer (en collection) : ({:service "vostok_input.get_rate", :metric 220.3497222222222, :state "ok"})

;(fetch-stats {:host "hbase-i-ad8c8300.prod.aws.rtgi.eu" :port 60010} )
; (get-in (fetch-stats {}) [:beans 15 :MemHeapUsedM])
;(fetch-stats {})
;(filter #(= (:name %) "Hadoop:service=HBase,name=JvmMetrics")(:beans(fetch-stats {})) )
;(select-keys ["ThreadsTimedWaiting" "MemMaxM"]) (filter #(= (:name %) "Hadoop:service=HBase,name=JvmMetrics")(:beans(fetch-stats {})) ))

;name à prendre : "Hadoop:service=HBase,name=JvmMetrics" "java.lang:type=Threading" "java.nio:type=BufferPool,name=direct"

;(map #(select-keys % [:ThreadContentionMonitoringSupported]) (fetch-stats-filtred "java.lang:type=Threading"))
;(map #(select-values % [:ThreadContentionMonitoringSupported]) (fetch-stats-filtred "java.lang:type=Threading"))
;cauchy-hbase.core=> (first(fetch-stats-filtred "java.lang:type=Threading"))
;cauchy-hbase.core=> (get (first(fetch-stats-filtred "java.lang:type=Threading")) :ObjectMonitorUsageSupported)
