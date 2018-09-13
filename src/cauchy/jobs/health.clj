(ns cauchy.jobs.health
  (:require [cauchy.jobs.utils :as utils]
            [clojure.tools.logging :as log]
            [clojure.string :as str]
            [sigmund.core :as sig])
  (:import [java.util ConcurrentModificationException]))

(def total-mem (:total (sig/os-memory)))

(def default-load-thresholds
  {"load_1"   {:warn 5 :crit 10 :comp >}
   "load_5"   {:warn 4 :crit 8 :comp >}
   "load_15"  {:warn 3 :crit 6 :comp >}
   "relative" {:warn 2 :crit 4 :comp >}})

(defn load-average
  ([{:keys [thresholds] :as conf}]
   (let [thresholds (merge-with merge default-load-thresholds thresholds)
         services ["load_1" "load_5" "load_15" "relative"]
         metrics (vec (sig/os-load-avg))
         core-count (:total-cores (first (sig/cpu)))
         relative_load (/ (first metrics) core-count)
         metrics (conj metrics relative_load)]
     (map (fn [s m]
            {:service s
             :metric m
             :state (utils/threshold (get thresholds s) m)})
          services metrics)))
  ([] (load-average {})))

(defn cpu-usage
    ([{:keys [warn crit] :as conf :or {warn 80 crit 90}}]
        (let [data (sig/cpu-usage)]
        (apply concat (map-indexed
            (fn [idx val]
                (let [{:keys [nice soft-irq idle irq user sys wait stolen] :as core-data} val]
                    [{:service (str "cpu.usage." idx ".nice") :metric nice}
                    {:service (str "cpu.usage." idx ".soft-irq") :metric soft-irq}
                    {:service (str "cpu.usage." idx ".user") :metric user}
                    {:service (str "cpu.usage." idx ".idle") :metric idle}
                    {:service (str "cpu.usage." idx ".sys") :metric sys}
                    {:service (str "cpu.usage." idx ".wait") :metric wait}
                    {:service (str "cpu.usage." idx ".irq") :metric irq}
                    {:service (str "cpu.usage." idx ".stolen") :metric stolen}]))
            data))))
    ([] (cpu-usage {})))

(defn memory
  ([{:keys [warn crit] :as conf :or {warn 80 crit 90}}]
   (let [{:keys [actual-used used-percent] :as data} (sig/os-memory)]
     [{:service "total" :metric total-mem}
      {:service "used" :metric actual-used}
      {:service "used_pct" :metric used-percent
       :state (utils/threshold {:comp > :crit crit :warn warn}
                               used-percent)}]))
  ([] (memory {})))

(defn uptime
  ([_]
   (let [{:keys [uptime] :as data} (sig/os-uptime)]
     [{:service "uptime" :metric uptime}]))
  ([] (uptime {})))

(defn swap
  ([{:keys [warn crit] :as conf :or {warn 80 crit 90}}]
   (let [{:keys [total used] :as data} (sig/os-swap)]
     (when-not (zero? total)
       (let [free (- total used)
             used-pct (double (* 100 (/ used total)))
             tconf {:comp > :crit crit :warn warn}]
         [{:service "total" :metric total}
          {:service "free" :metric free}
          {:service "used" :metric used}
          {:service "used_pct" :metric used-pct
           :state (utils/threshold tconf used-pct)}]))))
  ([] (swap {})))

(defn disk-entry
  [{:keys [warn crit] :as conf :or {warn 80 crit 90}}
   {:keys [dir-name dev-name] :as device}]
  (let [{:keys [total free]} (sig/fs-usage dir-name)]
    (when (and (pos? total) (pos? free))
      (let [total (bit-shift-left total 10) ;; kB
            free (bit-shift-left free 10) ;; kB
            used  (- total free)
            used-pct (double (* 100 (/ used total)))
            tconf {:comp > :crit crit :warn warn}
            sname (str/replace dir-name #"\/" "_")]
        [{:service (str sname ".total")
          :metric total}

         {:service (str sname ".free")
          :metric free}

         {:service (str sname ".used")
          :metric used}

         {:service (str sname ".used_pct")
          :metric used-pct
          :state (utils/threshold tconf used-pct)}]))))

(defn disk
  ([tconf]
   (let [virtual-fses ["/dev" "/sys" "/proc" "/run"]]
     (try
       (->> (sig/fs-devices)
          (remove (fn [{:keys [^String dir-name] :as entry}]
                    (some #(.startsWith dir-name %)
                          virtual-fses)))
          (map #(disk-entry tconf %))
          (flatten))
          (catch ConcurrentModificationException e
            {}))))
  ([] (disk {})))

(defn process
  [{:keys [pattern warn-num crit-num
           warn-cpu crit-cpu warn-mem crit-mem]
    :or {warn-num "1:1" crit-num "1:1"
         warn-cpu 10 crit-cpu 20
         warn-mem 10 crit-mem 20}}]

  (if pattern
    (let [all-pids (sig/os-pids)
          all-info (map (fn [pid]
                          (try
                            (merge {:cmd (str/join " " (sig/ps-args pid))}
                                   (sig/ps-cpu pid)
                                   (sig/ps-exe pid)
                                   (sig/ps-memory pid)
                                   (sig/ps-info pid))
                            (catch Exception e
                              nil)))
                        all-pids)
          all-info (remove nil? all-info)
          total-proc-count (count all-info)
          patt (re-pattern pattern)
          matched-process (filter #(re-find patt (:cmd %)) all-info)
          process-count (count matched-process)

          [nwl nwh] (map #(Integer/parseInt %) (str/split warn-num #"\:"))
          [ncl nch] (map #(Integer/parseInt %) (str/split crit-num #"\:"))
          final-state (utils/worst-state
                       (utils/threshold {:warn nwh :crit nch :comp >} process-count)
                       (utils/threshold {:warn nwl :crit ncl :comp <} process-count))

          number-msg {:service "num"
                      :metric process-count
                      :state final-state}

          sum-cpu (* 100 (reduce + (map :percent matched-process)))
          cpu-msg {:service "cpu"
                   :metric sum-cpu
                   :state (utils/threshold
                           {:warn warn-cpu :crit crit-cpu :comp >}
                           sum-cpu)}

          sum-rss (reduce + (map :rss matched-process))
          rss-msg {:service "rss"
                   :metric sum-rss}

          mem-used (double (/ (* 100 sum-rss) total-mem))
          mem-msg {:service "mem"
                   :metric mem-used
                   :state (utils/threshold
                           {:warn warn-mem :crit crit-mem :comp >}
                           mem-used)}]
      (remove nil? [number-msg cpu-msg rss-msg mem-msg]))
    ;; badly configured, need name and pattern
    (throw (Exception. (str "process check is badly configured: need pattern key")))))

(defn disk-io
  ([{:keys [r-warn r-crit w-warn w-crit] :as conf
     :or {r-warn 10000000 r-crit 10000000
          w-warn 20000000 w-crit 20000000}}]
   (let [devices (doall (sig/fs-devices))
         usage (doall (map #(sig/fs-usage (:dir-name %))
                           devices))
         reads  (->> usage (map :disk-read-bytes) (reduce +))
         writes (->> usage (map :disk-write-bytes) (reduce +))
         read-io (utils/rate [:disk-io :read] reads)
         write-io (utils/rate [:disk-io :write] writes)]
     [{:service "read_bytes_rate"
       :metric read-io
       :state (utils/threshold
               {:warn r-warn :crit r-crit :comp >}
               read-io)}
      {:service "write_bytes_rate"
       :metric write-io
       :state (utils/threshold
               {:warn w-warn :crit w-crit :comp >}
               write-io)}]))
  ([] (disk-io {})))

;;; getting all interfaces name, excluding localhost one
(defn listifasset []
 (set (sig/net-if-names)) )

;;; picking bandwidth for each interface
(defn listbandwidthperinterface []
	(let [x (listifasset) ]
		(apply list (map (fn [iname] (try
                                    (sig/net-if-usage iname)
                                    (catch Exception e
                                      (log/error "[IFACE]" iname " - " e)))) x))))

;;; main bandidth per interface function. Graph it with derivative and scaletosecond functions
(defn bandwidthperif []
	(vec(apply concat (map (fn [{:keys [rx-bytes tx-bytes name]}]
	[{:service (str name".rx-bytes") :state "ok" :metric rx-bytes }
     {:service (str name".tx-bytes") :state "ok" :metric tx-bytes }]
	) (listbandwidthperinterface) ))))

(defn bandwidth
  ([{:keys [rx-warn rx-crit tx-warn tx-crit] :as conf
     :or {rx-warn 5000000 rx-crit 10000000
          tx-warn 5000000 tx-crit 10000000}}]
   (when-let [{:keys [speed]} (sig/net-bandwidth)]
     (let [{:keys [rx-bytes tx-bytes]} speed]

       [{:service "rx_bytes_rate"
         :metric rx-bytes
         :state (utils/threshold
                 {:warn rx-warn :crit rx-crit :comp >}
                 rx-bytes)}

        {:service "tx_bytes_rate"
         :metric tx-bytes
         :state (utils/threshold
                 {:warn tx-warn :crit tx-crit :comp >}
                 tx-bytes)}])))
  ([] (bandwidth {})))
