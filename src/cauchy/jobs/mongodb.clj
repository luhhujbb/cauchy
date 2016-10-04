(ns cauchy.jobs.mongodb
  (:require [cheshire.core :refer :all]
            [clojure.string :as str])
  (:use [clojure.java.shell :only [sh]]))

(def mongostatsastring  
	(:out (sh "bash" "-c" "echo \"JSON.stringify(db.runCommand( { serverStatus: 1, repl: 0, metrics: 0, locks: 0 } ))\" |sudo mongo --quiet --host mongodb-ns315134.dev.ovh.rtgi.eu" )))

(def mongostatsasjson
	(parse-string mongostatsastring true))

(def getOpCountersMetrics 
	(hash-map
		:opcounters_command	(get-in mongostatsasjson [:opcounters :command])
		:opcounters_insert	(get-in mongostatsasjson [:opcounters :insert])
		:opcounters_update	(get-in mongostatsasjson [:opcounters :update])
		:opcounters_delete	(get-in mongostatsasjson [:opcounters :delete])
	))

(def getGlobalLockMetrics
	(hash-map
		:GlobalLock_currentQueue_writers (get-in mongostatsasjson [:globalLock :currentQueue :writers])
		:GlobalLock_currentQueue_readers (get-in mongostatsasjson [:globalLock :currentQueue :readers])
		:GlobalLock_activeClients_writers (get-in mongostatsasjson [:globalLock :activeClients :writers])
		:GlobalLock_activeClients_readers (get-in mongostatsasjson [:globalLock :activeClients :readers])
	))

(def getConnectionsMetrics
	(hash-map
		:connections_available (get-in mongostatsasjson [:connections :available])
		:connections_current (get-in mongostatsasjson [:connections :current])
	))

(def getMemMetrics
	(hash-map
		:mem_virtual (get-in mongostatsasjson [:mem :virtual])
		:mem_resident (get-in mongostatsasjson [:mem :resident])
	))

(def getWiredTigerConcurrentTransactionMetrics
	(hash-map
		:wiredTiger_concurrentTransactions_read_available (get-in mongostatsasjson [:wiredTiger :concurrentTransactions :read :available])
		:wiredTiger_concurrentTransactions_read_out (get-in mongostatsasjson [:wiredTiger :concurrentTransactions :read :out])
		:wiredTiger_concurrentTransactions_write_available (get-in mongostatsasjson [:wiredTiger :concurrentTransactions :write :available])
		:wiredTiger_concurrentTransactions_write_out (get-in mongostatsasjson [:wiredTiger :concurrentTransactions :write :out])
	))

(def getWiredTigerCacheMetrics
	(hash-map
		:wiredTiger_cache_bytes_currently_in_the_cache (get-in mongostatsasjson [:wiredTiger :cache (keyword "bytes currently in the cache")])
	))

(def getAssertsMetrics
	(hash-map
		:asserts_warning (get-in mongostatsasjson [:asserts :warning])
		:asserts_msg (get-in mongostatsasjson [:asserts :msg])
		:asserts_user (get-in mongostatsasjson [:asserts :user])
		:asserts_rollovers (get-in mongostatsasjson [:asserts :rollovers])
		:asserts_regular (get-in mongostatsasjson [:asserts :regular])
	))

(def mergeallstats 
	(merge 
		getOpCountersMetrics 
		getGlobalLockMetrics 
		getConnectionsMetrics 
		getWiredTigerConcurrentTransactionMetrics 
		getWiredTigerCacheMetrics getMemMetrics 
		getAssertsMetrics))

(defn mongodb-metrics [] 
	(let [{:keys [:asserts_msg :asserts_regular :connections_current :asserts_user 
				:wiredTiger_concurrentTransactions_write_out :wiredTiger_concurrentTransactions_read_available
				:mem_virtual :GlobalLock_activeClients_readers :GlobalLock_currentQueue_writers
				:opcounters_insert :opcounters_command :wiredTiger_cache_bytes_currently_in_the_cache
				:GlobalLock_currentQueue_readers :GlobalLock_activeClients_writers :opcounters_delete
				:opcounters_update :wiredTiger_concurrentTransactions_read_out :connections_available
				:asserts_rollovers :asserts_warning :wiredTiger_concurrentTransactions_write_available
				:mem_resident]} mergeallstats]
		[
		 {:service (str "asserts.msg") :metric asserts_msg }
		 {:service (str "asserts.regular") :metric asserts_regular }
		 {:service (str "connections.current") :metric connections_current }
		 {:service (str "asserts.user") :metric asserts_user }
		 {:service (str "wiredTiger.concurrentTransactions.write.out") :metric wiredTiger_concurrentTransactions_write_out }
		 {:service (str "wiredTiger.concurrentTransactions.read.available") :metric wiredTiger_concurrentTransactions_read_available }
		 {:service (str "mem.virtual") :metric mem_virtual }
		 {:service (str "GlobalLock.activeClients.readers") :metric GlobalLock_activeClients_readers }
		 {:service (str "GlobalLock.currentQueue.writers") :metric GlobalLock_currentQueue_writers }
		 {:service (str "opcounters.insert") :metric opcounters_insert }
		 {:service (str "opcounters.command") :metric opcounters_command }
		 {:service (str "wiredTiger.cache.bytesCurrentlyInTheCache") :metric wiredTiger_cache_bytes_currently_in_the_cache }
		 {:service (str "GlobalLock.currentQueue.readers") :metric GlobalLock_currentQueue_readers }
		 {:service (str "GlobalLock.activeClients.writers") :metric GlobalLock_activeClients_writers }
		 {:service (str "opcounters.delete") :metric opcounters_delete }
		 {:service (str "opcounters.update") :metric opcounters_update }
		 {:service (str "wiredTiger.concurrentTransactions.read.out") :metric wiredTiger_concurrentTransactions_read_out }
		 {:service (str "connections.available") :metric connections_available }
		 {:service (str "asserts.rollovers") :metric asserts_rollovers }
		 {:service (str "asserts.warning") :metric asserts_warning }
		 {:service (str "wiredTiger.concurrentTransactions.write.available") :metric wiredTiger_concurrentTransactions_write_available }
		 {:service (str "mem.resident") :metric mem_resident }
		]))
