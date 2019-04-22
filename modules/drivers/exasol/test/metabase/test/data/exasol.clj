(ns metabase.test.data.exasol
  "Code for creating / destroying a Exasol database from a `DatabaseDefinition`."
  (:require [clojure.java.jdbc :as jdbc]
            [metabase.driver.sql-jdbc.connection :as sql-jdbc.conn]
            [metabase.test.data
             [interface :as tx]
             [sql :as sql.tx]
             [sql-jdbc :as sql-jdbc.tx]]
            [metabase.test.data.sql-jdbc
             [execute :as execute]
             [load-data :as load-data]]
            [metabase.util :as u]))

(sql-jdbc.tx/add-test-extensions! :exasol)

(defmethod sql.tx/field-base-type->sql-type [:exasol :type/BigInteger] [_ _] "BIGINT")
(defmethod sql.tx/field-base-type->sql-type [:exasol :type/Boolean]    [_ _] "BOOLEAN")
(defmethod sql.tx/field-base-type->sql-type [:exasol :type/Char]       [_ _] "VARCHAR(254)")
(defmethod sql.tx/field-base-type->sql-type [:exasol :type/Date]       [_ _] "DATE")
(defmethod sql.tx/field-base-type->sql-type [:exasol :type/DateTime]   [_ _] "TIMESTAMP")
(defmethod sql.tx/field-base-type->sql-type [:exasol :type/Decimal]    [_ _] "NUMERIC")
(defmethod sql.tx/field-base-type->sql-type [:exasol :type/Float]      [_ _] "FLOAT")
(defmethod sql.tx/field-base-type->sql-type [:exasol :type/Integer]    [_ _] "INTEGER")
(defmethod sql.tx/field-base-type->sql-type [:exasol :type/Text]       [_ _] "VARCHAR(254)")
(defmethod sql.tx/field-base-type->sql-type [:exasol :type/Time]       [_ _] "TIME")

(defn- db-name []
  (tx/db-test-env-var-or-throw :exasol :db "docker"))

(def ^:private db-connection-details
  (delay {:host     (tx/db-test-env-var-or-throw :exasol :host "localhost")
          :port     (Integer/parseInt (tx/db-test-env-var-or-throw :exasol :port "5433"))
          :user     (tx/db-test-env-var :exasol :user "dbadmin")
          :password (tx/db-test-env-var :exasol :password)
          :db       (db-name)
          :timezone :America/Los_Angeles}))

(defmethod tx/dbdef->connection-details :exasol [& _] @db-connection-details)

(defmethod sql.tx/qualified-name-components :exasol
  ([_ _]                             [(db-name)])
  ([_ db-name table-name]            ["public" (tx/db-qualified-table-name db-name table-name)])
  ([_ db-name table-name field-name] ["public" (tx/db-qualified-table-name db-name table-name) field-name]))

(defmethod sql.tx/create-db-sql         :exasol [& _] nil)
(defmethod sql.tx/drop-db-if-exists-sql :exasol [& _] nil)

(defmethod sql.tx/drop-table-if-exists-sql :exasol [& args]
  (apply sql.tx/drop-table-if-exists-cascade-sql args))

(defmethod load-data/load-data! :exasol [& args]
  (apply load-data/load-data-one-at-a-time-parallel! args))

(defmethod sql.tx/pk-sql-type :exasol [& _] "INTEGER")

(defmethod execute/execute-sql! :exasol [& args]
  (apply execute/sequentially-execute-sql! args))

(defmethod tx/has-questionable-timezone-support? :exasol [_] true)


(defn- dbspec []
  (sql-jdbc.conn/connection-details->spec :exasol @db-connection-details))

(defmethod tx/before-run :exasol [_]
  ;; Close all existing sessions connected to our test DB
  (jdbc/query (dbspec) "SELECT CLOSE_ALL_SESSIONS();")
  ;; Increase the connection limit; the default is 5 or so which causes tests to fail when too many connections are made
  (jdbc/execute! (dbspec) (format "ALTER DATABASE \"%s\" SET MaxClientSessions = 10000;" (db-name))))
