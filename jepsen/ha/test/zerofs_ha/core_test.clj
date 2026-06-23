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
