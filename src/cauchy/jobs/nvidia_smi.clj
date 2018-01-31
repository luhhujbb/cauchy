(ns cauchy.jobs.nvidia-smi
  (:require [clojure.java.shell :as shell]
            [clojure.string :as str]
            [clojure.xml :as xml]
            [clojure.java.io :as io]))


(defn remove-dtd
  [s]
  (let [patt "<!DOCTYPE nvidia_smi_log SYSTEM \"nvsmi_device_v9.dtd\">"]
  (str/replace s (re-pattern patt) "")))

(defn get-tags
  [xml-map tag]
  (filter (fn [x] (= tag (:tag x))) (:content xml-map)))

(defn nvidia-smi
  [file-path]
  (let [res (shell/sh "nvidia-smi" "-x" "-q")]
    (xml/parse (io/input-stream (.getBytes (remove-dtd (:out res)))))))

(defn parse-long
  [istr]
  (if-not (= istr "N/A")
    (let [percent (first (str/split istr #" "))]
      (Long/parseLong percent))
    -1))

(defn get-gpu-usage
  [gpu-data]
  (let [raw-data (first (get-tags (first (get-tags gpu-data :gpu)) :utilization))]
    (into [] (map (fn [x] {:service (str "gpu." (name (:tag x)) "_percent")
                            :metric (parse-long (first (:content x)))})
                            (:content raw-data)))))

(defn get-gpu-temperature
  [gpu-data]
  (let [raw-data (first (get-tags (first (get-tags gpu-data :gpu)) :temperature))]
    (into [] (map (fn [x] {:service (str "gpu.temp." (name (:tag x)) "_celsius")
                            :metric (parse-long (first (:content x)))})
                            (:content raw-data)))))

(defn gpu-data
  []
  (let [gpu-data (nvidia-smi)]
    (into []
      (concat
        (get-gpu-usage gpu-data)
        (get-gpu-temperature gpu-data)))))
