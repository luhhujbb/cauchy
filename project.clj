(defproject cauchy "0.1.98"
  :description "Cauchy is an agent for Riemann"
  :url "https://github.com/pguillebert/cauchy"
  :scm {:name "git"
        :url "https://github.com/pguillebert/cauchy"}
  :pom-addition [:developers
                 [:developer
                  [:name "Philippe Guillebert"]
                  [:url "https://github.com/pguillebert"]
                  [:timezone "+1"]]]
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :signing {:gpg-key "93FEB8D7"}
  :deploy-repositories [["clojars" {:creds :gpg}]]
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [org.clojure/tools.logging "0.3.1"]
                 [org.slf4j/slf4j-api "1.7.19"]
                 [org.slf4j/slf4j-log4j12 "1.7.19"]
                 [org.slf4j/jcl-over-slf4j "1.7.19"]
                 [log4j/log4j "1.2.17"]
                 [log4j/apache-log4j-extras "1.2.17"]
                 [riemann-clojure-client "0.4.5"]
                 [com.google.protobuf/protobuf-java  "2.6.1"]
                 [com.soundcloud/prometheus-clj "2.4.1"]
                 [jarohen/chime "0.2.2"]
                 [cheshire "5.8.0"]
                 [clj-http "2.3.0"]
                 [com.climate/claypoole "1.1.4"]
                 [indigenous "0.1.0"]
                 [bultitude "0.2.8"]
                 [ring-server "0.5.0"]
                 [luhhujbb/sigmund "0.2.2" :exclusions [log4j sigar/sigar-native-deps]]
                 [luhhujbb/hbase-admin "0.1.8"]
                 [clj-sockets "0.1.0"]
                 [org.clojure/java.jmx "0.3.4"]
                 [org.hyperic/sigar "1.6.5.132-6"]]
    :repositories {"redhat ga" "https://maven.repository.redhat.com/ga/"}
  :aot :all
  :main cauchy.core)
