(ns cauchy.jobs.sysctl
  (:require [clojure.walk]
			[cheshire.core :refer :all]
            [clojure.string :as str])
  (:use [clojure.java.shell :only [sh]]))

(def defaultstate "ok")
(defn getsysctlstats []
	(:out (sh "bash" "-c" "sudo sysctl -a --pattern 'netfilter'" )))

(defn replacedots []
    (clojure.string/replace (getsysctlstats) #"\." "_"))

(defn sysctlasmap []
 (into {} (map (fn [s] 
			(clojure.string/split s #" = ")) 
  (clojure.string/split (replacedots)  #"\n" ))
))

(defn keywordizemap []
(clojure.walk/keywordize-keys (sysctlasmap)))

(defn sysctlmetrics []
 (let [{:keys [:net_netfilter_nf_conntrack_count
               :net_netfilter_nf_conntrack_max ]} (keywordizemap)]
			  (cond
				(= net_netfilter_nf_conntrack_count net_netfilter_nf_conntrack_max) (def state "critical")
				 :else (def state "ok"))
[{:service "conntrack.connection_count" :state state :metric (Long/parseLong net_netfilter_nf_conntrack_count)}
 {:service "conntrack.connection_max" :state defaultstate :metric (Long/parseLong net_netfilter_nf_conntrack_max)}
    ]))
