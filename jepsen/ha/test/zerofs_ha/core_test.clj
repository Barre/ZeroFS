(ns zerofs-ha.core-test
  (:require [clojure.test :refer [deftest is testing]]
            [jepsen.checker :as checker]
            [zerofs-ha.core :as core]))

(defn- check [history]
  (checker/check (core/set-checker) {} history {}))

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
