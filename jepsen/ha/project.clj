(defproject zerofs-ha "0.1.0-SNAPSHOT"
  :description "Jepsen HA test for ZeroFS: a leader/standby pair over MinIO, with
                a nemesis that kills the leader, the standby, both, or MinIO and
                heals back to a healthy cluster, checked for no-acked-loss."
  :url "https://github.com/Barre/ZeroFS"
  :license {:name "AGPL-3.0"}
  :main zerofs-ha.core
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [jepsen "0.3.7"]]
  :jvm-opts ["-Djava.awt.headless=true"
             "-Xmx2g"]
  :repl-options {:init-ns zerofs-ha.core})
