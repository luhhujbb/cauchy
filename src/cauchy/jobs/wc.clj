(ns cauchy.jobs.wc
  (:require [clojure.java.shell :as shell]
            [clojure.string :as str]
            [clojure.java.io :as io]))

(defn wc
  [file-path]
  (let [res (shell/sh "wc" "-l" file-path)]
    (Long/parseLong
      (first (str/split (:out res) #"\s")))))

(defn get-files-lines
  [fs]
  (map
    (fn [f]
      [(.getName f) (wc (.getAbsolutePath f))])
    (remove
      (fn [f]
      (.isDirectory f))
      fs)))

(defn rm-ext
  [file-name remove-extension]
  (if remove-extension
    (let [sp-st (str/split file-name #"\.")]
      (str/join "." (drop-last sp-st)))
    file-name))

(defn file-lines
  [{:keys [path remove-extension] :as conf}]
  (let [fp (io/file path)
        fs (file-seq fp)
        files-count (get-files-lines fs)]
        (into []
          (map
            (fn [[file-name ct]]
              {:service (rm-ext file-name remove-extension) :metric ct})
            files-count))))
