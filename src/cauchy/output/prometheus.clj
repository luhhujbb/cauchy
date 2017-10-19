(ns cauchy.output.prometheus
  (:require [prometheus.core :as prometheus]
            [ring.server.standalone :refer [serve]]
            [clojure.tools.logging :as log])
  (:import (java.io IOException)))

;;Store the prometheus registry
(def store (atom nil))

;;Atom to store registered metrics (allow auto metrics registration)
(def metric-registry (atom {}))

(def namespace (atom "cauchy"))

(defn init! [conf]
  (->> (prometheus/init-defaults)
       (reset! store)))

(defn send! [msg])
