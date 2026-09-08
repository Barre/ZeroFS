(ns zerofs-ha.core-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing]]
            [jepsen.checker :as checker]
            [jepsen.client :as client]
            [jepsen.db :as db]
            [jepsen.generator.interpreter :as interpreter]
            [jepsen.nemesis :as nemesis]
            [jepsen.util :as util]
            [slingshot.slingshot :refer [try+]]
            [zerofs-ha.core :as core])
  (:import [java.net StandardProtocolFamily UnixDomainSocketAddress]
           [java.nio.channels ServerSocketChannel]
           [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(defn- check [history]
  (checker/check (core/set-checker) {} history {}))

(deftest nemesis-errors-invalidate-the-experiment
  (let [check #(checker/check (core/nemesis-checker) {} % {})
        history [{:type :info :process :nemesis :f :partition :value :partitioned}
                 {:type :info :process 0 :f :write :error "Stale file handle"}]]
    (is (:valid? (check history)))
    (doseq [error [{:error :timeout}
                  {:exception {:class "clojure.lang.ExceptionInfo"}}]]
      (let [op (merge {:type :info :process :nemesis :f :await-serving} error)
            result (check (conj history op))]
        (is (false? (:valid? result)))
        (is (= 1 (:error-count result)))
        (is (= [(dissoc op :type :process)] (:errors result)))))))

(deftest mount-client-config-defaults-to-fuse
  (is (= "fuse" (:mount-client (core/cfg {:work-dir "/tmp/zerofs-ha"}))))
  (is (= "native" (:mount-client
                    (core/cfg {:work-dir "/tmp/zerofs-ha"
                               :mount-client "native"})))))

(deftest mount-dispatches-to-the-selected-client
  (let [events (atom [])
        awaits (atom 0)
        base   {:work "/tmp/zerofs-ha"
                :zerofs "/tmp/zerofs"
                :mount "/tmp/zerofs-ha/mnt"
                :nodes {:a {:ninep "/tmp/a.sock"}
                        :b {:ninep "/tmp/b.sock"}}}]
    (with-redefs [core/daemon-start!
                  (fn [& args] (swap! events conj [:daemon args]))
                  core/sh-ok!
                  (fn [& args] (swap! events conj [:shell args]) {:exit 0})
                  core/mounted? (constantly true)
                  util/await-fn
                  (fn [ready? _]
                    (swap! awaits inc)
                    (is (true? (ready?))))]
      (core/mount! (assoc base :mount-client "fuse"))
      (core/mount! (assoc base :mount-client "native")))
    (is (= 2 @awaits))
    (is (= [[:daemon
             ["/tmp/zerofs-ha/mount.pid"
              "/tmp/zerofs-ha/mount.log"
              {}
              "/tmp/zerofs"
              ["mount"
               "unix:/tmp/a.sock,unix:/tmp/b.sock"
               "/tmp/zerofs-ha/mnt"
               "--writeback"
               "false"]]]
            [:shell
             [:sudo
              :mount
              :-t
              :zerofs
              :-o
              "consistency=strict,msize=10485760"
              "unix:/tmp/a.sock,unix:/tmp/b.sock"
              "/tmp/zerofs-ha/mnt"]]]
           @events))))

(deftest durability-handles-close-once
  (let [closed  (atom [])
        resource (fn [name]
                   (proxy [java.io.Closeable] []
                     (close [] (swap! closed conj name))))
        handles (atom {"file" {:raf (resource :file)}
                       "dir"  {:dirchan (resource :dir)}})]
    (core/close-durability-handles! handles)
    (core/close-durability-handles! handles)
    (is (= {} @handles))
    (is (= #{:file :dir} (set @closed)))
    (is (= 2 (count @closed)))))

(deftest durability-handles-survive-indeterminate-operations-and-worker-close
  (doseq [operation [:write :truncate]]
    (let [dir (.toFile (Files/createTempDirectory "zerofs-ha-test"
                                                (make-array FileAttribute 0)))
          file (io/file dir "h")
          fail? (atom true)
          closed (atom 0)
          raf (proxy [java.io.RandomAccessFile] [file "rw"]
                (write [data]
                  (if (compare-and-set! fail? true false)
                    (throw (java.io.IOException. "Stale file handle"))
                    (proxy-super write data)))
                (setLength [length]
                  (if (compare-and-set! fail? true false)
                    (throw (java.io.IOException. "Stale file handle"))
                    (proxy-super setLength length)))
                (close [] (swap! closed inc) (proxy-super close)))
          handles (atom {"alias" {:raf raf :file "h"}})
          c (core/->DurabilityClient (.getPath dir) handles (atom 0))
          test {:nodes ["n1"] :client c}
          worker (interpreter/open (interpreter/client-nemesis-worker) test 0)
          other (interpreter/open (interpreter/client-nemesis-worker) test 1)]
      (try
        (let [failed (interpreter/invoke! worker test
                                          {:type :invoke :f operation :as "alias"
                                           :to 7 :process 0})]
          (is (= {:type :info :file "h" :error "Stale file handle"
                  :value (if (= :write operation) 1 7)}
                 (select-keys failed [:type :file :error :value]))))
        ;; Jepsen advances the process after :info, including timeout results.
        ;; Use its actual ClientWorker so this exercises client replacement.
        (is (= :ok
               (:type (interpreter/invoke! worker test
                                          {:type :invoke :f :write :as "alias"
                                           :process 2}))))
        (is (= :ok
               (:type (interpreter/invoke! other test
                                          {:type :invoke :f :fsync :as "alias"
                                           :process 1}))))
        (interpreter/close! worker test)
        (is (zero? @closed) "one worker cannot close the shared handles")
        (is (= :ok
               (:type (interpreter/invoke! other test
                                          {:type :invoke :f :write :as "alias"
                                           :process 1}))))
        (is (= :ok
               (:type (interpreter/invoke! other test
                                          {:type :invoke :f :fsync :as "alias"
                                           :process 1}))))
        (let [result (interpreter/invoke! other test
                                           {:type :invoke :f :read :process 1})]
          (is (= #{"h"} (set (keys (:value result))))))
        (client/teardown! c test)
        (client/teardown! c test)
        (is (empty? @handles))
        (is (= 1 @closed) "test teardown closes each handle once")
        (finally
          (interpreter/close! worker test)
          (interpreter/close! other test)
          (client/teardown! c test)
          (.delete file)
          (.delete dir))))))

(deftest await-serving-requires-the-targets-new-writer-epoch
  (doseq [[leader standby] [[:a :b] [:b :a]]
          fault [:partition :kill-leader]]
    (let [epochs (atom {leader 7 standby nil})
          probes (atom [])
          cuts (atom 0)
          n (core/ha-nemesis)
          invoke (fn [f] (nemesis/invoke! n {} {:type :info :f f}))]
      (with-redefs [core/cfg (constantly {})
                    core/cluster-roles (atom {leader :leader standby :standby})
                    core/relays (atom {:to-a {:cut! #(swap! cuts inc)}
                                      :to-b {:cut! #(swap! cuts inc)}})
                    core/kill-pid! (fn [_])
                    core/node-writer-epoch
                    (fn [_ node] (swap! probes conj node) (get @epochs node))
                    ;; Even a listening old leader must not satisfy the wait.
                    core/node-9p-up? (constantly true)
                    util/await-fn
                    (fn [ready? _]
                      (is (thrown? clojure.lang.ExceptionInfo (ready?)))
                      (swap! epochs assoc standby 7)
                      (is (thrown? clojure.lang.ExceptionInfo (ready?))
                          "the previous epoch is not a promotion")
                      (swap! epochs assoc standby 8)
                      (is (true? (ready?))))]
        (invoke fault)
        (is (= (if (= :partition fault) 2 0) @cuts))
        (is (= [leader] @probes) "capture the writer before injecting the fault")
        (reset! probes [])
        (is (= :serving (:value (invoke :await-serving))))
        (is (= [standby standby standby] @probes))
        (is (= standby (core/leader-node)))))))

(deftest unmount-dispatches-to-the-selected-client
  (let [commands (atom [])
        base     {:mount "/tmp/zerofs-ha/mnt"}]
    (with-redefs [core/mounted? (constantly true)
                  core/sh! (fn [& args]
                             (swap! commands conj args)
                             {:exit 0})
                  core/sh-ok! (fn [& args]
                                (swap! commands conj args)
                                {:exit 0})]
      (core/unmount! (assoc base :mount-client "fuse"))
      (core/unmount! (assoc base :mount-client "native")))
    (is (= [[:fusermount3 :-uz "/tmp/zerofs-ha/mnt"]
            [:sudo :umount "/tmp/zerofs-ha/mnt"]]
           @commands))))

(deftest node-9p-up-requires-a-listener
  (let [dir         (.toFile (Files/createTempDirectory
                              "zerofs-ha-test"
                              (make-array FileAttribute 0)))
        socket      (io/file dir "ninep.sock")
        socket-path (.getPath socket)
        c           {:nodes {:a {:ninep socket-path}}}]
    (try
      (spit socket "")
      (is (false? (core/node-9p-up? c :a)))
      (is (.delete socket))
      (with-open [server (ServerSocketChannel/open StandardProtocolFamily/UNIX)]
        (.bind server (UnixDomainSocketAddress/of socket-path))
        (is (true? (core/node-9p-up? c :a))))
      (finally
        (.delete socket)
        (.delete dir)))))

(deftest standby-ready-count-is-node-specific
  (let [log (java.io.File/createTempFile "zerofs-ha-test" ".log")
        c   {:nodes {:a {:log (.getPath log)}
                     :b {:log (.getPath log)}}}]
    (try
      (spit log (str "HA standby a: watching leader heartbeats\n"
                     "HA standby b: watching leader heartbeats\n"
                     "HA standby b: watching leader heartbeats\n"))
      (is (= 1 (core/standby-ready-count c :a)))
      (is (= 2 (core/standby-ready-count c :b)))
      (finally
        (.delete log)))))

(deftest node-replication-up-requires-a-listener
  (let [server (java.net.ServerSocket. 0 1 (java.net.InetAddress/getLoopbackAddress))
        c      {:nodes {:b {:repl-port (.getLocalPort server)}}}]
    (try
      (is (true? (core/node-replication-up? c :b)))
      (finally
        (.close server)))
    (is (false? (core/node-replication-up? c :b)))))

(deftest setup-waits-for-the-standby-receiver-before-starting-the-leader
  (let [started        (atom {})
        receiver-ready (atom false)
        standby-ready  (atom 4)
        mounted        (atom false)]
    (with-redefs [core/cluster-roles (atom {})
                  core/relays (atom nil)
                  core/cluster-down! (fn [_])
                  core/sh! (fn [& _] {:exit 0})
                  core/start-minio! (fn [_])
                  core/make-bucket! (fn [_])
                  core/start-relay! (fn [& _] {})
                  core/standby-ready-count (fn [_ _] @standby-ready)
                  core/start-node!
                  (fn [_ node-key role]
                    (when (= :a node-key)
                      (is @receiver-ready
                          "leader background writes must have a reachable standby"))
                    (swap! started assoc node-key role))
                  core/node-replication-up?
                  (fn [_ node-key]
                    (is (= :b node-key))
                    (is (= {:b "standby"} @started))
                    @receiver-ready)
                  core/node-9p-up? (fn [_ _] (= "leader" (:a @started)))
                  core/mount!
                  (fn [_]
                    (is (= 5 @standby-ready) "mount must wait for this standby startup")
                    (reset! mounted true))
                  util/await-fn
                  (fn [ready? options]
                    (case (:log-message options)
                      "Waiting for standby replication listener"
                      (do (is (thrown? clojure.lang.ExceptionInfo (ready?)))
                          (reset! receiver-ready true))
                      "Waiting for standby b"
                      (do (is (= {:a "leader" :b "standby"} @started))
                          (is (thrown? clojure.lang.ExceptionInfo (ready?)))
                          (swap! standby-ready inc))
                      nil)
                    (is (true? (ready?))))]
      (db/setup! (core/db) {:work-dir "/tmp/zerofs-ha-test"} "n1")
      (is @mounted)
      (is (= {:a :leader :b :standby} @core/cluster-roles)))))

(deftest make-bucket-waits-for-a-readable-bucket
  (let [commands (atom [])
        stats    (atom 0)
        c        {:mc "/mc" :minio-addr "127.0.0.1:9000"
                  :access-key "key" :secret-key "secret" :bucket "bucket"}]
    (with-redefs [core/sh-ok!
                  (fn [& args]
                    (swap! commands conj args)
                    (when (and (= "stat" (second args))
                               (= 1 (swap! stats inc)))
                      (throw (ex-info "bucket not ready" {})))
                    {:exit 0})
                  util/await-fn
                  (fn [ready? _]
                    (try
                      (ready?)
                      (catch clojure.lang.ExceptionInfo _
                        (ready?))))]
      (core/make-bucket! c)
      (is (= ["alias" "mb" "stat" "alias" "mb" "stat"]
             (mapv second @commands))))))

(deftest heal-restart-discovers-either-leader-and-waits-for-its-standby
  (doseq [[leader standby] [[:a :b] [:b :a]]]
    (let [started (atom [])
          killed (atom [])
          counted (atom #{})
          counts (atom {:a 4 :b 7})
          epochs (atom {})
          roles {:a :dead :b :dead}
          awaits (atom 0)]
      (with-redefs [core/cfg (constantly {})
                    core/cluster-roles (atom roles)
                    core/minio-up? (constantly true)
                    core/node-pid (fn [_ node] node)
                    core/kill-pid! #(swap! killed conj %)
                    core/standby-ready-count
                    (fn [_ node]
                      (swap! counted conj node)
                      (get @counts node))
                    core/start-node!
                    (fn [_ node role]
                      (is (= #{:a :b} @counted)
                          "capture both standby baselines before either startup")
                      (swap! started conj [node role]))
                    core/node-writer-epoch (fn [_ node] (get @epochs node))
                    ;; A listening but non-authoritative node must not be chosen.
                    core/node-9p-up? (constantly true)
                    core/mounted? (constantly true)
                    util/await-fn
                    (fn [ready? _]
                      (is (= roles @core/cluster-roles)
                          "publish roles only after the pair is ready")
                      (case (swap! awaits inc)
                        1 (do
                            (is (= [[:b "standby"] [:a "leader"]] @started)
                                "both receivers must start before waiting")
                            (is (thrown? clojure.lang.ExceptionInfo (ready?)))
                            (swap! epochs assoc leader 11)
                            (let [selected (ready?)]
                              (is (= leader selected))
                              selected))
                        2 (do
                            (is (thrown? clojure.lang.ExceptionInfo (ready?))
                                "old standby log entries cannot satisfy recovery")
                            (swap! counts update standby inc)
                            (is (true? (ready?))))
                        3 (is (true? (ready?)))))]
        (core/heal-restart! {})
        (is (= [:a :b] @killed))
        (is (= {leader :leader standby :standby} @core/cluster-roles))
        ;; The next fault must target the elected writer, including when b won.
        (reset! killed [])
        (nemesis/invoke! (core/ha-nemesis) {} {:type :info :f :kill-leader})
        (is (= [leader] @killed))))))

(deftest set-checker-flags-lost-and-resurrected
  (testing "catches a lost add (present per ops, missing from the final read) and a
            resurrected remove (absent per ops, present in the final read), while
            ignoring indeterminate (:info) values"
    (let [r (check [{:type :ok   :f :add    :value 1}        ; present, will be read -> ok
                    {:type :ok   :f :add    :value 2}        ; added then removed -> absent, ok
                    {:type :ok   :f :remove :value 2}
                    {:type :ok   :f :add    :value 3}        ; present per ops but NOT read -> LOST
                    {:type :ok   :f :add    :value 4}        ; removed but present in read -> RESURRECTED
                    {:type :ok   :f :remove :value 4}
                    {:type :info :f :add    :value 5}        ; indeterminate -> skipped either way
                    {:type :fail :f :remove}                 ; no :value -> ignored
                    {:type :ok   :f :read   :value (sorted-set 1 4 5)}])]
      (is (false? (:valid? r)))
      (is (= [3] (:lost r)))
      (is (= [4] (:resurrected r))))))

(deftest set-checker-passes-a-consistent-history
  (testing "a history where every present/absent value matches the final read is valid"
    (let [r (check [{:type :ok   :f :add    :value 1}        ; present + read
                    {:type :ok   :f :add    :value 2}        ; removed + not read
                    {:type :ok   :f :remove :value 2}
                    {:type :info :f :add    :value 3}        ; indeterminate, happens to be read
                    {:type :ok   :f :read   :value (sorted-set 1 3)}])]
      (is (true? (:valid? r)))
      (is (zero? (:lost-count r)))
      (is (zero? (:resurrected-count r))))))

(deftest set-checker-info-after-ok-is-indeterminate
  (testing "an :info op after an :ok add makes the value indeterminate (not asserted)"
    (let [r (check [{:type :ok   :f :add    :value 1}
                    {:type :info :f :remove :value 1}        ; might have removed -> indeterminate
                    {:type :ok   :f :read   :value (sorted-set)}])]  ; absent, but that's allowed
      (is (true? (:valid? r))))))

(deftest set-checker-bounds-anomaly-samples
  (let [lost-values        (range 60)
        resurrected-values (range 100 160)
        history            (concat
                            (map (fn [v] {:type :ok :f :add :value v}) lost-values)
                            (mapcat (fn [v] [{:type :ok :f :add :value v}
                                             {:type :ok :f :remove :value v}])
                                    resurrected-values)
                            [{:type :ok :f :read :value (into (sorted-set)
                                                              resurrected-values)}])
        r                  (check history)]
    (is (false? (:valid? r)))
    (is (= 60 (:lost-count r)))
    (is (= (vec (range 50)) (:lost r)))
    (is (= 60 (:resurrected-count r)))
    (is (= (vec (range 100 150)) (:resurrected r)))))

(deftest add-error-classification
  (testing "ENOENT/EEXIST from the rename are indeterminate: a resent rename whose
            original applied re-executes exactly this way. The NIO two-path
            exceptions carry a null reason (message is just \"src -> dst\", no
            errno text), so classification must not depend on the message"
    (is (= {:type :info :error :not-durable-here}
           (core/classify-add-error
            (java.nio.file.NoSuchFileException. "/mnt/d1/.tmp-9" "/mnt/d1/9" nil))))
    (is (= {:type :info :error :retried-create}
           (core/classify-add-error
            (java.nio.file.FileAlreadyExistsException. "/mnt/d1/.tmp-9" "/mnt/d1/9" nil)))))
  (testing "single-path java.io exceptions still classify by message text"
    (is (= {:type :info :error :not-durable-here}
           (core/classify-add-error
            (java.io.FileNotFoundException. "/mnt/d1/9 (No such file or directory)"))))
    (is (= {:type :info :error :retried-create}
           (core/classify-add-error
            (java.io.IOException. "File exists")))))
  (testing "other IO errors are indeterminate after a multi-step add"
    (is (= :info (:type (core/classify-add-error
                         (java.io.IOException. "Bad file descriptor")))))
    (is (= :info (:type (core/classify-add-error
                         (java.io.IOException. "Input/output error")))))))

(deftest inode-accounting-waits-for-replay-protected-orphans
  (let [samples (atom [20 19])
        options (atom nil)]
    (with-redefs [core/count-all-inodes (constantly 20)
                  core/statfs-used-inodes
                  (fn [_]
                    (let [sample (first @samples)]
                      (swap! samples rest)
                      sample))
                  util/await-fn
                  (fn [f opts]
                    (reset! options opts)
                    (try+
                      (f)
                      (catch [:type ::core/inode-reclamation-pending] pending
                        (is (= 20 (:used-inodes pending)))
                        (is (= 19 (:expected-used-inodes pending)))
                        (f))))]
      (is (= 19 (core/await-inode-accounting!
                 "/mnt" {:used-inodes 17 :all-inodes 18})))
      (is (empty? @samples))
      (is (= core/inode-accounting-timeout-ms (:timeout @options)))
      (is (> (:timeout @options) 150000)))))

(deftest statfs-checker-retains-the-orphan-leak-check
  (let [history [{:type :ok :f :read :value #{}
                  :used-inodes 17 :all-inodes 18}
                 {:type :ok :f :read :value #{1 2}
                  :used-inodes 20 :all-inodes 20}]
        result (checker/check (core/statfs-checker) {} history {})]
    (is (false? (:valid? result)))
    (is (= 3 (:inodes-grew result)))
    (is (= 2 (:census-grew result)))))
