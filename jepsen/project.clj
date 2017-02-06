(defproject nsq "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :repositories [["custom-repo-for-nsq" "http://maven.example-self.com/repositories/"]]
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/core.async "0.2.395"]
                 [cheshire "5.7.0"]
                 [jepsen "0.1.4"]
                 [clj-http "2.3.0"]
                 [slingshot "0.12.2"]
                 [com.youzan/NSQ-Client "2.2.1027-RELEASE"]
                 [org.apache.httpcomponents/httpclient "4.5.2"]
                 [org.apache.commons/commons-pool2 "2.4.2"]
                 [ch.qos.logback/logback-core "1.1.7"]
                 [ch.qos.logback/logback-classic "1.1.7"]
                 [org.slf4j/jul-to-slf4j "1.7.21"]
                 [org.slf4j/jcl-over-slf4j "1.7.21"]
                 [org.slf4j/slf4j-api "1.7.21"]])
