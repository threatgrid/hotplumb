(defproject hotplumb "0.0.1-SNAPSHOT"
  :description "A core.async library in a similar spirit to prismatic/plumbing"
  :url "http://github.com/threatgrid/hotplumb"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :source-paths ["src/clj" "src/cljs" "src/cljx"
                 "target/generated_sources/cljx/clj"]
  :test-paths ["test/clj" "test/cljs" "test/cljx"
               "target/generated_sources/cljx/clj"
               "target/generated_test_sources/cljx/clj"]
  :jar-exclusions [#"[.]cljx$|[.]sw.$"]
  :plugins [[lein-cljsbuild "1.0.3"]
            [com.keminglabs/cljx "0.3.2"]]
  :hooks [cljx.hooks]
  :cljx {:builds [{:source-paths ["src/cljx"]
                   :output-path "target/generated_sources/"
                   :rules :clj}
                  {:source-paths ["src/cljx"]
                   :output-path "target/generated_sources/cljx/cljs"
                   :rules :cljs}
                  {:source-paths ["src/cljx" "test/cljx"]
                   :output-path "target/generated_test_sources/cljx/clj"
                   :rules :clj}
                  {:source-paths ["src/cljx" "test/cljx"]
                   :output-path "target/generated_test_sources/cljx/cljs"
                   :rules :cljs}]}
  :cljsbuild {:builds [{:source-paths ["src/cljs" "target/generated_sources/cljx/cljs"]
                        :compiler {:output-to "resources/public/generated/web/dev/cljs-out/main.js"
                                   :output-dir "resources/public/generated/web/dev/cljs-out"
                                   :source-map "resources/public/generated/web/dev/cljs-out/main.smap"
                                   :optimizations :none
                                   :pretty-print true}}
                       ]}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/core.async "0.1.303.0-886421-alpha"]
                 [aysylu/loom "0.4.3-SNAPSHOT"]
                 [com.taoensso/timbre "3.2.1"]
                 [org.clojure/tools.namespace "0.2.4"]]
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}
             :prod {:cljsbuild {:builds [{:source-paths ["src/cljs" "target/generated_sources/cljx/cljs"]
                                          :compiler {:output-to "resources/generated/web/prod/cljs-out/main.js"
                                                     :output-dir "resources/generated/web/prod/cljs-out"
                                                     :optimizations :advanced
                                                     :pretty-print false}}]}}}
  ;; TODO: move this into a dev profile?
  :jvm-opts ["-agentlib:jdwp=transport=dt_socket,server=y,suspend=n"])
