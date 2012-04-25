(ns sql-data-migrator.main
    (:gen-class)
    (:require 
     [clojure.java.jdbc :as jdbc]
     [clojure.java.jdbc.internal :as jdbci])
    (:use 
     [clojure.pprint]
     [clojure.tools.cli])
    (:import
     (java.sql Connection )))


(def ^:dynamic config
     { :source { :classname "com.mysql.jdbc.Driver"
                 :subprotocol "mysql"
                 :subname "//localhost:3306/madek_prod"
                 :user "root"
                 :password ""}
       :target { :classname "org.postgresql.Driver"
                 :subprotocol "postgresql"
                 :subname "//localhost:5432/madek_prod"
                 :user "postgres"
                 :password ""}
       :disable_triggers false
       :reset_pg_sequences true
       :tables nil
                 })

(defn disect-seq-name [seq-name]
  (let [splitted (clojure.string/split  seq-name #"_")
        column-name (last (pop splitted))
        table-name (clojure.string/join "_" (pop (pop splitted)))]
    {:table-name table-name, :column column-name}))


(defn reset-pg-auto-sequences [db-spec]
  (jdbc/with-connection db-spec
    (jdbc/with-query-results res [(str "SELECT relname from pg_class where relkind = 'S'")]
      (doseq [rec res]
        (let [h (disect-seq-name (:relname rec))
              cmd (str " select setval('" (:table-name h) "_id_seq',(SELECT max(" (:column h) ") from " (:table-name h)")); " )]
          (println (str "executing "  cmd))
          (.executeQuery (jdbc/prepare-statement (:connection jdbci/*db*) cmd))
          )))))

; clojure.java.jdbc binds the with-connection opened connection to its single
; internal variable *db*. It is therefore quite painfull to work with more than
; one open connection at a time. We maintain explicit references like
; source-con-obj and target-con-obj.
(defn transfer-data [config]
  (jdbc/with-connection (:source config) 
    (def source-con-obj jdbci/*db*)
    (jdbc/with-connection (:target config) 
      (jdbc/transaction
        (def target-con-obj jdbci/*db*)
        (doseq [table-name  
                (or (:tables config)
                (map #(:table_name %) (jdbc/resultset-seq 
                        (.getTables (.getMetaData (:connection source-con-obj)) nil nil "%" nil))))]

            (if (:disable_triggers config)
              (if (= (:subprotocol (:target config)) "postgresql")
                (jdbc/do-commands (str "ALTER TABLE " table-name " DISABLE TRIGGER ALL; "))))

            (println (str "transfering " table-name))

            (doseq [rec (jdbc/resultset-seq 
                          (.executeQuery (jdbc/prepare-statement (:connection source-con-obj) 
                             (str "SELECT * FROM " table-name))))]
              (jdbc/insert-record table-name rec))
            
            (if (:disable_triggers config)
              (if (= (:subprotocol (:target config)) "postgresql")
                (jdbc/do-commands (str "ALTER TABLE " table-name " ENABLE TRIGGER ALL; "))))

              )))))


(defn list-tables [db-spec]
  (jdbc/with-connection db-spec
          (map #(:table_name %) (doall 
                (jdbc/resultset-seq (.getTables 
                    (.getMetaData (:connection jdbci/*db*)) nil nil "%" nil))))))


(defn do-it [config]
  (do
      (println "sql data migrator")
      (println "using the following configuration: ")
      (pprint config)
      (println "transfering data...")
      (transfer-data config)
      (if (:reset_pg_sequences config)
        (do 
          (println "resetting sequences ...")
          (reset-pg-auto-sequences (:target config))))))

(defn -main [& args]


  (let [[opts args banner]
        (cli args
            ["-c" "--config" "Path to the configuration file"]
            ["-p" "--print-config" "Print an example configuration and exit" :default false :flag true]
            ["-h" "--help" "Show help and exit" :default false :flag true]
            ["-t" "--list-tables" "List tables from source and exit" :default false :flag true])]


    (cond
      (:print-config opts)(do 
                            (pprint config)
                            (flush)
                            (System/exit 0))
      (:help opts)(do 
                    (println banner)
                    (flush)
                    (System/exit 0))
      (:list-tables opts)(do
                          (pprint (list-tables (:source config)))
                          (flush)
                          (System/exit 0)))

    (def config (load-string (slurp (:config opts)))))

  (do-it config))





