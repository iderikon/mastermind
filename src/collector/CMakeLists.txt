add_executable(mastermind-collector
    Discovery.cpp Node.cpp Parser.cpp ProcfsParser.cpp
    BackendParser.cpp Metrics.cpp ThreadPool.cpp
    Storage.cpp Group.cpp Couple.cpp DiscoveryTimer.cpp
    ConfigParser.cpp WorkerApplication.cpp CocaineHandlers.cpp
    Namespace.cpp FS.cpp FilterParser.cpp Backend.cpp
    Filter.cpp main.cpp)

target_link_libraries(mastermind-collector
    ${elliptics_cpp_LIBRARY} ${elliptics_client_LIBRARY}
    ${CocaineNative_LIBRARIES} ${Cocaine_LIBRARY}
    ${CURL_LIBRARIES} boost_system boost_thread msgpack rt)

add_custom_target(tarball
    COMMAND tar czf mastermind-collector.tar.gz mastermind-collector
    DEPENDS mastermind-collector)

