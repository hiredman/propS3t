(ns propS3t.support
  (:require [clojure.set :as set]
            [clj-http.core :as http]
            [clj-http.client :as c]
            [propS3t.signing :as s])
  (:import (java.util Date)
           (java.text SimpleDateFormat FieldPosition)
           (java.io PipedOutputStream
                    PipedInputStream)))

;; if you have your own date-formater bind this so you are not making
;; requests all the time
(def ^{:dynamic true} *date-formater* nil)

(def date-format "EEE, d MMM yyyy HH:mm:ss Z")

(defn date-formater []
  (SimpleDateFormat. date-format))

(defn date []
  (let [date (Date.)
        sb (StringBuffer.)]
    (.toString (.format (or *date-formater* (date-formater))
                        date sb (FieldPosition. 0)))))

(defn extract-key-data [item]
  (set/rename-keys 
   (let [snag {:ETag (comp first :content)
               :Key (comp first :content)
               :LastModified (comp first :content)}]
     (reduce
      (fn [out element]
        (if (contains? snag (:tag element))
          (assoc out (:tag element)
                 ((get snag (:tag element)) element))
          out))
      {} (:content item)))
   {:ETag :etag
    :Key :key
    :LastModified :last-modified}))

(defmacro xml-extract [roots path-spec fun]
  (let [root (gensym 'root)
        {:keys [out prev]}
        (reduce
         (fn [{:keys [out prev]} fun]
           (let [[prev form] (fun prev)]
             {:prev prev
              :out (into out form)}))
         {:prev root
          :out []}
         (for [[fun p] (partition-all 2 path-spec)]
           (fn [pitem]
             (let [item (gensym 'item)]
               [item `[:when (= (:tag ~pitem) ~p)
                       ~item (~fun ~pitem)]]))))
        last-v (peek out)
        out (pop out)
        last-b (peek out)
        out (pop out)]
    `(for ~(into [root roots] out)
       (~fun ~last-v))))

(defn extract-multipart-upload-data [item]
  (set/rename-keys
   (let [snag {:Bucket (comp first :content)
               :Key (comp first :content)
               :UploadId (comp first :content)}]
     (reduce
      (fn [out element]
        (if (contains? snag (:tag element))
          (assoc out (:tag element)
                 ((get snag (:tag element)) element))
          out))
      {} item))
   {:Bucket :bucket
    :Key :key
    :UploadId :upload-id}))

(defn multipart-xml-fragment [parts]
  (let [result (StringBuffer.)]
    (.append result "<CompleteMultipartUpload>")
    (doseq [{:keys [part tag]} parts]
      (doto result
        (.append "<Part>")
        (.append "<PartNumber>")
        (.append part)
        (.append "</PartNumber>")
        (.append "<ETag>")
        (.append tag)
        (.append "</ETag>")
        (.append "</Part>")))
    (.append result "</CompleteMultipartUpload>")
    (.toString result)))

(def request (-> http/request
                 c/wrap-query-params
                 c/wrap-url
                 c/wrap-exceptions
                 s/wrap-aws-signature))

(defn pipe []
  (let [pin (PipedInputStream.)
        pout (PipedOutputStream. pin)]
    {:inputstream pin :outputstream pout}))
