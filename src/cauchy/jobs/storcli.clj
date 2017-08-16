(ns cauchy.jobs.storcli
  (:require [clojure.java.shell :as shell]
            [clojure.string :as str]
            [cheshire.core :as json]))

(defn parse-string
  "Remove space and parenthesis from json keys, lowercase everything"
  [json]
  (json/parse-string
    json
    (fn [k]
      (-> k
          (.replace "(" " ")
          (.replace ")" " ")
          (.replace "/" " ")
          (.replace "_" " ")
          str/trim
          .toLowerCase
          (.replaceAll " +" "-")
          keyword))))

(defn usefull-data
  [json]
  (get-in
      (parse-string json)
      [:controllers
       0
      :response-data]))

(defn vdrive-state->riemann-state
  [in]
  (condp = in
   "Optl" "ok"
   "Pdgd" "warning"
   "Rec" "warning"
   "dgrd" "critical"
   "OfLn" "critical"
   "warning"))

(defn pdrive-state->riemann-state
[in]
 (condp = in
  "Onln" "ok"
  "Offln" "critical"
  "warning"))

(def yes-no->riemann-state
 {"Yes" "ok"
  "No" "warning"})

(defn ->byte-size
  [number unit]
  (condp = unit
    "KB" (Math/round (* 1000 number))
    "MB" (Math/round (* 10000000 number))
    "GB" (Math/round (* 1000 1000000 number))
    "TB" (Math/round (* 1000000 1000000 number))))

(defn get-bytes-nb
  [in]
  (let [[nb unit] (str/split in #" ")]
    (->byte-size (Double/parseDouble nb) unit)))

(defn get-storcli-cmd
  []
  (let [res (shell/sh "which" "StorCli")
        res* (if (= 1 (:exit res))
              (shell/sh "which" "storcli")
              res)
        path (if (= 0 (:exit res))
            (str/replace (:out res) #"\n" "")
            nil  )]
   path))


 (defn get-storcli-virtual-drive-data
  [sudo ctl-id vd-id]
  (if-let [path (get-storcli-cmd)]
    (usefull-data (:out (if sudo
            (shell/sh "sudo" path (str "/c" ctl-id) (str "/v" vd-id) "show" "all" "J")
            (shell/sh path (str "/c" ctl-id) (str "/v" vd-id) "show" "all" "J"))))
    nil))

(defn get-storcli-controller-data
 [sudo ctl-id]
 (if-let [path (get-storcli-cmd)]
   (usefull-data (:out (if sudo
           (shell/sh "sudo" path (str "/c" ctl-id) "show" "all" "J")
           (shell/sh path (str "/c" ctl-id) "show" "all" "J"))))
   nil))

(defn get-storcli-controllers-data
  [sudo]
  (if-let [path (get-storcli-cmd)]
    (usefull-data (:out (if sudo
            (shell/sh "sudo" path "show" "all" "J")
            (shell/sh path "show" "all" "J"))))
    nil))

(defn virt-drive-data->phy-drive-metrics
  [cid vdid data]
  (let [metric-path (str "c" cid ".v" vdid ".d" (:did data) ".")]
  [{:service (str metric-path "state")
    :state (pdrive-state->riemann-state (:state data))}
   {:service (str metric-path "size")
    :metric (get-bytes-nb (:size data))}]))

(defn virt-drive-data->phy-drives-metrics
  [cid vdid data]
  (mapcat
    (fn [pd-data]
      (virt-drive-data->phy-drive-metrics cid vdid pd-data))
    data))

(defn controller-data->virt-drive-metrics
  [sudo cid vdid data]
  (let [virt-drive-data (get-storcli-virtual-drive-data sudo cid vdid)
        virt-drive-properties ((keyword (str "vd" vdid "-properties")) virt-drive-data)
        phy-drive-kw (keyword (str "pds-for-vd-" vdid))
        metric-path (str "c" cid ".v" vdid ".")]
    (concat
      ;;TODO
      [{:service (str metric-path "state")
        :state (vdrive-state->riemann-state (:state data))}
       {:service (str metric-path "consistency")
        :state (yes-no->riemann-state (:consist data))}
       {:service (str metric-path "size")
        :metric (get-bytes-nb (:size data))}
       {:service (str metric-path "drive_nb")
        :metric (count (phy-drive-kw virt-drive-data))}]
      (virt-drive-data->phy-drives-metrics
        cid
        vdid
        (phy-drive-kw virt-drive-data)))))

(defn controller-data->virt-drives-metrics
  [sudo cid data]
  (mapcat
    (fn [vd]
      (let [[dg-id vd-id] (str/split (:dg-vd vd) #"/")]
        (controller-data->virt-drive-metrics sudo cid vd-id vd)))
      data))

(defn controller-data->roc-metrics
  [cid data]
  [{:service (str "c" cid ".roc.temperature_celcius") :metric (:roc-temperature-degree-celcius data)}])

(defn controller-data->bbu-metrics
  [cid data]
  [{:service (str "c" cid ".bbu.temperature_celcius")
    :metric (Long/parseLong
              (.replace
                (:temp (first data))
                "C" ""))}])

(defn controller-data->metrics
  [sudo data]
  (let [basics (:basics data)
        status (:status data)
        cid (:controller basics)]
  (concat
    [{:service (str "c" cid ".physical_drives") :metric (:physical-drives data)}
     {:service (str "c" cid ".virtual_drives") :metric (:virtual-drives data)}
     {:service (str "c" cid ".drive_groups") :metric (:drive-groups data)}
     {:service (str "c" cid ".status")
      :metric (if (= "OK" (:controller-status)) 1 -1)
      :state (if (= "OK" (:controller-status)) "ok" "warning")}]
    (controller-data->virt-drives-metrics sudo cid (:vd-list data))
    (controller-data->roc-metrics cid (:hwcfg data))
    (controller-data->bbu-metrics cid (:bbu-info data)))))

(defn storcli-data->metrics
  [sudo data]
  (into [] (concat
            [{:service "number-of-controllers"
              :metric (count data)}]
          (mapcat
            (fn [ctl-data]
                    (controller-data->metrics sudo ctl-data))
                    data))))

(defn storcli-raid-state
  ([{:keys [sudo]
     :or {sudo false}
     :as conf}]
     (let [adapters-summary (get-in
                              (get-storcli-controllers-data sudo)
                              [:system-overview])
           data (map
                  (fn [x]
                    (get-storcli-controller-data sudo (:ctl x)))
                    adapters-summary)]
           (into [] (storcli-data->metrics sudo data))))
 ([] (storcli-raid-state {})))
