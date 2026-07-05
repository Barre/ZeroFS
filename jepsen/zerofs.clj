(ns jepsen.local-fs.db.zerofs
  "A DB backed by a real ZeroFS server."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [db :as db]
                    [util :as util :refer [await-fn meh]]]
            [jepsen.local-fs [util :refer [sh sh*]]]
            [jepsen.local-fs.db.core :refer [LoseUnfsyncedWrites]]
            [slingshot.slingshot :refer [try+ throw+]]))

(def start-stop-daemon
  "Path to start-stop-daemon, which we use to daemonize the server and mount."
  "/usr/sbin/start-stop-daemon")

(defn paths
  "Given the mount directory, returns a map of all the paths we use. Server
  state (object store, cache, config, logs, pidfiles) lives in a sibling
  `<dir>.zerofs` directory so it's clearly separate from the mounted tree."
  [dir]
  (let [dir  (.getCanonicalPath (io/file dir))
        work (str dir ".zerofs")]
    {:mount      dir
     :work       work
     ; A fresh MinIO prefix per run isolates trials (MinIO has no per-run wipe,
     ; unlike the old file:// store that teardown's rm -rf cleaned).
     :store      (str "s3://zerofs-jepsen/run-" (java.util.UUID/randomUUID))
     :cache      (str work "/cache")
     :config     (str work "/config.toml")
     :server-log (str work "/server.log")
     :server-pid (str work "/server.pid")
     :mount-log  (str work "/mount.log")
     :mount-pid  (str work "/mount.pid")}))

(defn config-str
  "The ZeroFS config TOML for an S3-backed, 9P-only server."
  [{:keys [store cache password port]}]
  (str "[cache]\n"
       "dir = \"" cache "\"\n"
       "disk_size_gb = 1.0\n"
       "memory_size_gb = 0.25\n"
       "\n"
       "[storage]\n"
       "url = \"" store "\"\n"
       "encryption_password = \"" password "\"\n"
       "\n"
       "[aws]\n"
       "endpoint = \"http://127.0.0.1:9000\"\n"
       "access_key_id = \"minioadmin\"\n"
       "secret_access_key = \"minioadmin\"\n"
       "allow_http = \"true\"\n"
       "\n"
       "[servers.ninep]\n"
       "addresses = [\"127.0.0.1:" port "\"]\n"
       "\n"
       ; Crash-fault determinism: make an explicit fsync the only thing that
       ; flushes to the store. Otherwise the periodic flush would persist
       ; un-fsynced writes on its own, so a SIGKILL would keep more than the
       ; model (which reverts to the last fsync) expects.
       ; sync_writes stays at its default (false) so un-fsynced writes are lost.
       "[lsm]\n"
       "flush_interval_secs = 86400\n"))

(defn daemon-start!
  "Daemonizes a zerofs invocation via start-stop-daemon, writing its pid to
  pidfile and its stdout/stderr to log. `args` are the zerofs subcommand and
  flags (e.g. \"run\" \"-c\" config)."
  [bin pidfile log args]
  (sh :bash :-c
      (str start-stop-daemon
           " --start"
           " --background"
           " --no-close"
           " --make-pidfile"
           " --pidfile " pidfile
           " --exec " bin
           " -- " (str/join " " args)
           " >> " log " 2>&1")))

(defn pid
  "Reads a pidfile, returning the pid as a long, or nil if absent/unreadable."
  [pidfile]
  (try (-> pidfile slurp str/trim parse-long)
       (catch java.io.IOException _ nil)
       (catch RuntimeException _ nil)))

(defn kill!
  "Sends signal (a keyword/string like :KILL or :TERM) to the pid in pidfile,
  if any. Ignores missing processes."
  [signal pidfile]
  (when-let [p (pid pidfile)]
    (meh (sh :kill (str "-" (name signal)) (str p)))))

(defn listening?
  "Is something listening on the given TCP port on localhost?"
  [port]
  (try+ (sh :bash :-c (str "ss -ltn 'sport = :" port "' | grep -q LISTEN"))
        true
        (catch [:type :jepsen.local-fs.util/nonzero-exit] _ false)))

(defn mounted?
  "Is the given directory a ZeroFS mount?"
  [dir]
  (try+ (boolean (re-find #"zerofs" (sh :findmnt :-n dir)))
        (catch [:type :jepsen.local-fs.util/nonzero-exit] _ false)))

(defn unmount!
  "Unmounts the FUSE filesystem at dir, lazily, ignoring errors."
  [dir]
  (when (mounted? dir)
    (info "Unmounting" dir)
    (or (meh (sh :fusermount3 :-uz dir))
        (meh (sh :fusermount  :-uz dir)))))

(defrecord DB [bin port password mount work store cache config
               server-log server-pid mount-log mount-pid writeback]
  db/DB
  (setup! [this test node]
    (info "Setting up ZeroFS at" mount)
    (sh :mkdir :-p work cache mount)
    (spit config (config-str this))
    ; Start the server
    (daemon-start! bin server-pid server-log ["run" "-c" config])
    (await-fn (fn server-up [] (or (listening? port)
                                   (throw+ {:type ::server-not-listening
                                            :port port
                                            :log  (meh (slurp server-log))})))
              {:retry-interval 100
               :log-interval   5000
               :log-message    (str "Waiting for ZeroFS 9P server on :" port)})
    ; Mount it
    (daemon-start! bin mount-pid mount-log
                   ["mount" (str "127.0.0.1:" port) mount
                    "--writeback" (str writeback)])
    (await-fn (fn mount-up [] (or (mounted? mount)
                                  (throw+ {:type ::not-mounted
                                           :dir  mount
                                           :log  (meh (slurp mount-log))})))
              {:retry-interval 100
               :log-interval   5000
               :log-message    (str "Waiting for ZeroFS to mount at " mount)})
    (info "ZeroFS mounted at" mount))

  (teardown! [this test node]
    (info "Tearing down ZeroFS at" mount)
    (unmount! mount)
    (kill! :KILL mount-pid)
    (kill! :KILL server-pid)
    (meh (sh :bash :-c (str "rm -rf "
                            (pr-str work) " "
                            (pr-str mount)))))

  LoseUnfsyncedWrites
  ; Crash the server, losing the un-fsynced (memtable-resident) tail, then bring
  ; it back from the object store. The mount is torn down and re-established so
  ; no client-side state can mask the loss.
  (lose-unfsynced-writes! [this]
    (info "Losing un-fsynced writes: killing ZeroFS server at" mount)
    (unmount! mount)
    (kill! :KILL mount-pid)
    (kill! :KILL server-pid)
    (await-fn (fn server-down [] (or (not (listening? port))
                                     (throw+ {:type ::server-still-up})))
              {:retry-interval 100
               :log-interval   5000
               :log-message    "Waiting for killed server to stop listening"})
    ; Restart server against the same store, then remount.
    (daemon-start! bin server-pid server-log ["run" "-c" config])
    (await-fn (fn server-up [] (or (listening? port)
                                   (throw+ {:type ::server-not-listening})))
              {:retry-interval 100
               :log-interval   5000
               :log-message    "Waiting for restarted ZeroFS server"})
    (daemon-start! bin mount-pid mount-log
                   ["mount" (str "127.0.0.1:" port) mount
                    "--writeback" (str writeback)])
    (await-fn (fn mount-up [] (or (mounted? mount)
                                  (throw+ {:type ::not-mounted})))
              {:retry-interval 100
               :log-interval   5000
               :log-message    "Waiting for ZeroFS to remount"})
    :done))

(defn db
  "Constructs a ZeroFS-backed Jepsen DB from CLI options:

    :dir               Mount point (and, via <dir>.zerofs, the state root).
    :zerofs-bin        Path to the zerofs binary.
    :zerofs-port       TCP port for the 9P server.
    :zerofs-password   Encryption password.
    :zerofs-writeback  FUSE writeback cache (default false = write-through)."
  [{:keys [dir zerofs-bin zerofs-port zerofs-password zerofs-writeback]}]
  (let [bin (.getCanonicalPath (io/file zerofs-bin))]
    (when-not (.exists (io/file bin))
      (throw+ {:type ::no-binary
               :bin  bin
               :msg  (str "zerofs binary not found at " bin
                          "; build it (cargo build) or pass --zerofs-bin")}))
    (map->DB (merge (paths dir)
                    {:bin       bin
                     :port      zerofs-port
                     :password  zerofs-password
                     :writeback (boolean zerofs-writeback)}))))
