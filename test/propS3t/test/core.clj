(ns propS3t.test.core
  (:refer-clojure :exclude [key])
  (:load "aws") ;; aws credentials and test bucket name
  (:use [propS3t.core]
        [clojure.test])
  (:require [clojure.java.io :as io])
  (:import (java.util UUID)
           (java.io ByteArrayInputStream
                    ByteArrayOutputStream)))

(def ^{:dynamic true} *creds*)

(use-fixtures :once
              (fn [f]
                (create-bucket {:aws-key key
                                :aws-secret-key skey}
                               test-bucket)
                (binding [*creds* {:aws-key key
                                   :aws-secret-key skey}]
                  (f))))

(comment
  (deftest t-bucket-life-cycle
    (let [b (str (UUID/randomUUID))]
      (try
        (is (create-bucket {:aws-key key
                            :aws-secret-key skey}
                           b))
        (is (contains? (set (map :name (list-buckets *creds*))) b))
        (finally
         (is (delete-bucket {:aws-key key
                             :aws-secret-key skey}
                            b)))))))

(deftest t-test-bucket-exists
  (is (contains? (set (map :name (list-buckets *creds*))) test-bucket)))

(deftest t-write-stream
  (is (write-stream *creds* test-bucket "write-stream-test"
                    (ByteArrayInputStream.
                     (.getBytes "hello world"))
                    :length 11))
  (is (contains? (set (map :key (list-bucket *creds* test-bucket "" 10)))
                 "write-stream-test"))
  (dotimes [i 11]
    (is (write-stream *creds* test-bucket (str "write-stream-test" i)
                      (ByteArrayInputStream.
                       (.getBytes "hello world"))
                      :length 11)))
  (is (= 10 (count (list-bucket *creds* test-bucket "" 10))))
  (is (= 4 (count (list-bucket *creds* test-bucket "" 10
                               "write-stream-test5")))))

(deftest t-read-stream
  (is (write-stream *creds* test-bucket "write-stream-test"
                    (ByteArrayInputStream.
                     (.getBytes "hello world"))
                    :length 11))
  (with-open [baos (ByteArrayOutputStream.)]
    (io/copy (read-stream *creds* test-bucket "write-stream-test")
             baos)
    (is (= "hello world" (String. (.toByteArray baos)))))
  (with-open [baos (ByteArrayOutputStream.)]
    (io/copy (read-stream *creds* test-bucket "write-stream-test"
                          :offset 6)
             baos)
    (is (= "world" (String. (.toByteArray baos)))))
  (with-open [baos (ByteArrayOutputStream.)]
    (io/copy (read-stream *creds* test-bucket "write-stream-test"
                          :length 9
                          :offset 6)
             baos)
    (is (= "worl" (String. (.toByteArray baos)))))
  (with-open [baos (ByteArrayOutputStream.)]
    (io/copy (read-stream *creds* test-bucket "write-stream-test"
                          :length 4)
             baos)
    (is (= "hello" (String. (.toByteArray baos))))))
