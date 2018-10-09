(ns cauchy.jobs.iostat
  (:require [clojure.java.shell :as shell]
            [clojure.string :as str]
            [clojure.tools.logging :as log]))

(def base-metrics ["rrqm_per_s" "wrqm_per_s" "r_per_s" "w_per_s" "rkB_per_s" "wkB_s" "avgrq_sz" "avgqu_sz" "await" "r_await" "w_await" "svctm" "percent_util"])

(defn parse-iostat
    [iostat-res]
    (let [lines (str/split-lines iostat-res)
          lines-count (count lines)]
        (loop [ls lines
               pls []]
            (let [l (last ls)]
                (if (or (.startsWith l "Device") (nil? l))
                    pls
                    (let [splitted-line (str/split l #"\s+")]
                         (recur
                             (butlast ls)
                             (vec (conj
                                pls
                                [(first splitted-line) (apply
                                                            hash-map
                                                            (interleave
                                                                base-metrics
                                                                (rest splitted-line)))])))))))))

(defn dev-iostats->metrics
    [dev stats]
    (map (fn [[kv vv]]
                        {:service (str "iostats." dev "." kv)
                         :metric (Float/parseFloat (str/replace vv #"," "."))})
                    stats))

(defn iostat->metrics
    [iostat-res]
    (mapcat (fn [[dev stats]]
                (dev-iostats->metrics dev stats)) iostat-res))

(defn get-iostat
    "Time in second of report"
    ([{:keys [window] :or {window 30 }}]
        (if (> window 5)
            (let [iostat-raw-res (shell/sh "iostat" "-yx" (str (- window 5)) "1")]
                (iostat->metrics
                    (parse-iostat (:out iostat-raw-res))))
        []))
    ([] (get-iostat {})))
