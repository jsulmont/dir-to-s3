(ns s3-to-dir
  (:require
   [archive.directory :as dir]
   [clojure.java.io :as io]
   [clojure.tools.logging :as log])
  (:import
   java.util.function.BiFunction
   (software.amazon.awssdk.auth.credentials ProfileCredentialsProvider)
   [software.amazon.awssdk.core.async AsyncResponseTransformer]
   (software.amazon.awssdk.regions Region)
   (software.amazon.awssdk.services.s3 S3AsyncClient)
   [software.amazon.awssdk.services.s3.model GetObjectRequest NoSuchKeyException]))

(defn s3-to-dir [{:keys [client bucket key]} dest-dir]

  (let [input-stream
        (-> (.getObject client
                        (-> (GetObjectRequest/builder)
                            (.bucket bucket)
                            (.key key)
                            (.build))
                        (AsyncResponseTransformer/toBlockingInputStream))
            (.handle (reify BiFunction
                       (apply [_ resp e]
                         (if e
                           (try
                             (throw (.getCause ^Throwable e))
                             (catch NoSuchKeyException _
                               (log/warn "S3 key not found: " key))
                             (catch Exception e
                               (log/warnf e "Error fetching S3 object: s3://%s/%s" bucket key)))
                           resp))))
            (.join)
            io/input-stream)]
    (dir/restore-directory input-stream dest-dir)))

(comment

  (do

    (def s3-client (-> (S3AsyncClient/builder)
                       (.credentialsProvider (. ProfileCredentialsProvider create "jan-juxt"))
                       (.region (Region/EU-WEST-1))
                       (.build)))

    (def s3  {:client s3-client
              :bucket "jan-bucket1"
              :key "x/aws-doc-2023-01-25T07:43:30.547-00:00.tgz"}))

  (s3-to-dir s3 "/tmp/z")

  :ok)
