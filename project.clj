(defproject sql-data-migrator "1.0.0"
            :description "Tool to migrate data between RDMSes"
            :dependencies [
                           [mysql/mysql-connector-java "5.1.6"]
                           [org.clojure/clojure "1.3.0"]
                           [org.clojure/java.jdbc "0.1.4"]
                           [org.clojure/tools.cli "0.2.1"]
                           [postgresql/postgresql "9.1-901.jdbc4"]
                           ]
            :main sql-data-migrator.main
            )
