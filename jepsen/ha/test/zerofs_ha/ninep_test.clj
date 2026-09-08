(ns zerofs-ha.ninep-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is]]
            [zerofs-ha.ninep :as ninep])
  (:import [java.net StandardProtocolFamily UnixDomainSocketAddress]
           [java.nio.channels Channels ServerSocketChannel]
           [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(defn- wire-bytes [values]
  (byte-array (map unchecked-byte values)))

(defn- with-probe-server [respond check]
  (let [dir (.toFile (Files/createTempDirectory "zerofs-ha-probe"
                                              (make-array FileAttribute 0)))
        path (io/file dir "9p.sock")]
    (try
      (with-open [server (ServerSocketChannel/open StandardProtocolFamily/UNIX)]
        (.bind server (UnixDomainSocketAddress/of (.getPath path)))
        (let [peer (future
                     (with-open [channel (.accept server)]
                       (respond (Channels/newInputStream channel)
                                (Channels/newOutputStream channel))))]
          (try
            (check (.getPath path))
            (is (not= ::timeout (deref peer 3000 ::timeout)))
            (finally (future-cancel peer)))))
      (finally (.delete path) (.delete dir)))))

(defn- negotiate! [in out]
  (is (= (seq (wire-bytes [23 0 0 0 100 0 0 0 16 0 0 10 0
                           57 80 50 48 48 48 46 76 46 90]))
         (seq (.readNBytes in 23))))
  (.write out (wire-bytes [23 0 0 0 101 0 0 0 16 0 0 10 0
                          57 80 50 48 48 48 46 76 46 90])))

(deftest writer-epoch-requires-a-successful-lineage-response
  (doseq [[response expected]
          [[[23 0 0 0 234 1 0 11 0 0 0 0 0 0 0 29 0 0 0 0 0 0 0] 29]
           ;; A listener whose lease was revoked rejects Tgetlineage.
           [[11 0 0 0 7 1 0 107 0 0 0] nil]
           ;; An incomplete lineage response is not proof of authority.
           [[15 0 0 0 234 1 0 11 0 0 0 0 0 0 0] nil]]]
    (with-probe-server
      (fn [in out]
        (negotiate! in out)
        (is (= (seq (wire-bytes [7 0 0 0 233 1 0])) (seq (.readNBytes in 7))))
        (.write out (wire-bytes response)))
      #(is (= expected (ninep/writer-epoch %))))))

(deftest writer-epoch-times-out-a-listening-but-unresponsive-server
  (with-probe-server
    (fn [in _]
      ;; Never respond; the probe's timeout must close its connection.
      (while (not= -1 (.read in))))
    #(is (nil? (ninep/writer-epoch %)))))
