(ns cauchy.jobs.jmx
    (require [clojure.java.jmx :as jmx]
             [clojure.tools.logging :as log]
             [clojure.string :as str]))

(defn memory-stats [{:keys [host port] :or {host "localhost" port "9998"}}]
   (try
    (jmx/with-connection {:host host, :port port}
     (into []
      (mapcat 
       (fn [[k {:keys [committed max used] :as v}]] 
        (map (fn [[metric value]] 
         {:service (str (name k) "." (name metric)) :metric value}) 
        v)) 
      (jmx/read "java.lang:type=Memory" [:HeapMemoryUsage :NonHeapMemoryUsage]))))
   (catch Exception e (log/error "cant get mem stats for jmx on " host " port " port))))

(defn gc-stats [{:keys [host port gctype] :or {host "localhost" port "9998" gctype "PS MarkSweep"}}]
  (try
   (jmx/with-connection {:host host, :port port}
    (let [{:keys [GcThreadCount duration endTime memoryUsageBeforeGc memoryUsageAfterGc] :as json} (jmx/read (format "java.lang:type=GarbageCollector,name=%s" gctype) :LastGcInfo ) 
           gc-details (select-keys json [:memoryUsageBeforeGc :memoryUsageAfterGc])]
        (into []
         (apply concat
       [{:service (format "%s.GcThreadCount" (str/replace gctype #" " "_") ) :metric GcThreadCount}
        {:service (format "%s.duration" (str/replace gctype #" " "_") ) :metric duration}
        {:service (format "%s.endTime" (str/replace gctype #" " "_") ) :metric endTime}]
          (mapcat
            (fn [[memory-type vals-memory-type]]
              (map
                (fn [[rubrique-gc vals-rubrique-gc]] 
                [{:service (format "%s.%s.%s.committed" (str/replace gctype #" " "_") (name memory-type) (str/replace (name rubrique-gc)#" " "_")  ) :metric (:committed (:value vals-rubrique-gc))}
                {:service (format "%s.%s.%s.max" (str/replace gctype #" " "_") (name memory-type) (str/replace (name rubrique-gc)#" " "_")) :metric (:max (:value vals-rubrique-gc))}
                {:service (format "%s.%s.%s.used" (str/replace gctype #" " "_") (name memory-type) (str/replace (name rubrique-gc)#" " "_")) :metric (:used (:value vals-rubrique-gc))}])
               (select-keys vals-memory-type [(keyword "PS Eden Space") (keyword "PS Survivor Space") (keyword "PS Old Gen")] )))
           gc-details)))))
             (catch Exception e (log/error "cant get gc stats for jmx on " host " port " port))))

(defn threads-stats [{:keys [host port] :or {host "localhost" port "9998"}}]
    (try
     (jmx/with-connection {:host host, :port port}
       (let [{:keys [ThreadCount PeakThreadCount DaemonThreadCount]} (select-keys (jmx/mbean "java.lang:type=Threading") [:ThreadCount :PeakThreadCount :DaemonThreadCount])]
        [{:service (format "Threading.%s" (name 'ThreadCount)) :metric ThreadCount}
         {:service (format "Threading.%s" (name 'PeakThreadCount)) :metric PeakThreadCount}
         {:service (format "Threading.%s" (name 'DaemonThreadCount)) :metric DaemonThreadCount}]))
       (catch Exception e (log/error "cant get threads stats for jmx on " host " port " port))))


(defn stats [{:keys [host port] :or {host "localhost" port "9998"}}]
   (into []
    (concat
     (memory-stats {:host host :port port})
     (gc-stats {:host host :port port :gctype "PS MarkSweep"})
     (threads-stats {:host host :port port})
     (gc-stats {:host host :port port :gctype "PS Scavenge"}))))
