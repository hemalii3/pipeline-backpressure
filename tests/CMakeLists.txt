add_executable(pipeline_tests
    test_bounded_queue_mutex.cpp
    test_bounded_queue_lockfree.cpp
    test_pipeline_integration.cpp
)
target_link_libraries(pipeline_tests PRIVATE gtest_main Threads::Threads)

include(GoogleTest)
gtest_discover_tests(pipeline_tests)
