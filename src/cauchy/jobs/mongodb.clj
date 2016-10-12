(ns cauchy.jobs.mongodb
  (:require [cheshire.core :refer :all]
            [clojure.string :as str])
  (:use [clojure.java.shell :only [sh]]))

(defn mongostatsastring []
	(:out (sh "bash" "-c" "echo \"JSON.stringify(db.runCommand( { serverStatus: 1, repl: 0, metrics: 0, locks: 0 } ))\" |sudo mongo --quiet --host localhost" )))

(defn mongostatsasjson []
(parse-string (mongostatsastring) true)
)

(defn getopcounters []
	(let [{{:keys [insert delete command update]} :opcounters} (mongostatsasjson) ]
	[
	 { :service (str "opcounters.command") :metric command }
	 { :service (str "opcounters.insert") :metric insert }
	 { :service (str "opcounters.update") :metric update}
	 { :service (str "opcounters.delete") :metric delete}
	]
	))

(defn getgloballock []
	(let [{{:keys [currentQueue totalTime activeClients]
		   {:keys [writers readers]} :currentQueue } :globalLock } (mongostatsasjson) ]
   [
   { :service (str "currentQueue.writers") :metric writers }
   { :service (str "currentQueue.readers") :metric readers}
   ]
	))


(defn getasserts []
	(let [{{:keys [warning msg user rollovers regular]} :asserts} (mongostatsasjson) ]
	[
	 { :service (str "asserts.warning") :metric warning }
	 { :service (str "asserts.msg") :metric msg }
	 { :service (str "asserts.user") :metric user}
	 { :service (str "asserts.rollovers") :metric rollovers}
	 { :service (str "asserts.regular") :metric regular}
	]
	))

(defn getmem []
	(let [{{:keys [virtual resident]} :mem} (mongostatsasjson) ]
	[
	 { :service (str "mem.resident") :metric resident }
	 { :service (str "mem.virtual") :metric virtual }
	]
	))

(defn getconnection []
	(let [{{:keys [available current]} :connections} (mongostatsasjson) ]
	[
	 { :service (str "connections.available") :metric available }
	 { :service (str "connections.current") :metric current }
	]
	))

(defn getmem []
	(let [{{:keys [virtual resident]} :mem} (mongostatsasjson) ]
	[
	 { :service (str "mem.resident") :metric resident }
	 { :service (str "mem.virtual") :metric virtual }
	]
	))

(defn getwiredtigerconcurrentreadtransactions []
	(let [{{:keys [concurrentTransactions]
	  {:keys [read write]
    {:keys [available out]} :read } :concurrentTransactions } :wiredTiger } (mongostatsasjson) ]
  [
     { :service (str "wiredtiger.concurrenttransactions.read.available") :metric available }
     { :service (str "wiredtiger.concurrenttransactions.read.out") :metric out }
  ]
	))

(defn getwiredtigerconcurrentwritetransactions []
	(let [{{:keys [concurrentTransactions]
	  {:keys [read write]
    {:keys [available out]} :write } :concurrentTransactions } :wiredTiger } (mongostatsasjson) ]
  [
     { :service (str "wiredtiger.concurrenttransactions.write.available") :metric available }
     { :service (str "wiredtiger.concurrenttransactions.write.out") :metric out }
  ]
	))

(defn mongodb-metrics []
(into [] (concat (getwiredtigerconcurrentreadtransactions) (getwiredtigerconcurrentwritetransactions) (getmem) (getconnection) (getmem) (getasserts) (getopcounters) (getgloballock)))
  )
