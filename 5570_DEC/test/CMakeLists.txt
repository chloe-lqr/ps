include_directories(${PROJECT_SOURCE_DIR} ${HUSKY_EXTERNAL_INCLUDE})

add_library(csci5570 STATIC 
  $<TARGET_OBJECTS:server-objs> 
  $<TARGET_OBJECTS:comm-objs> 
  $<TARGET_OBJECTS:base-objs>
  $<TARGET_OBJECTS:io-objs>
  $<TARGET_OBJECTS:driver-objs>
  )

# Unit Test
file(GLOB_RECURSE UNITTEST_FILES ${PROJECT_SOURCE_DIR}/*_test.cpp) 
include_directories(${GTEST_INCLUDE})
add_executable(HuskyUnitTest ${UNITTEST_FILES} test_main.cpp)
add_dependencies(HuskyUnitTest gtest)
target_link_libraries(HuskyUnitTest csci5570)
target_link_libraries(HuskyUnitTest ${HUSKY_EXTERNAL_LIB})
target_link_libraries(HuskyUnitTest ${GTEST_LIBRARIES})
target_link_libraries(HuskyUnitTest ${GMOCK_LIBRARIES})
set_property(TARGET HuskyUnitTest PROPERTY CXX_STANDARD 11)
add_dependencies(HuskyUnitTest ${external_project_dependencies})

if(LIBHDFS3_FOUND)
	add_executable(TestRead test_hdfs_read.cpp)
	target_link_libraries(TestRead csci5570)
	target_link_libraries(TestRead ${HUSKY_EXTERNAL_LIB})
	set_property(TARGET TestRead PROPERTY CXX_STANDARD 11)
	add_dependencies(TestRead ${external_project_dependencies})
endif(LIBHDFS3_FOUND)

  add_executable(TestSimple simple_example.cpp)
  target_link_libraries(TestSimple csci5570)
  target_link_libraries(TestSimple ${HUSKY_EXTERNAL_LIB})
  target_link_libraries(TestSimple ${GTEST_LIBRARIES})
  target_link_libraries(TestSimple ${GMOCK_LIBRARIES})
  set_property(TARGET TestSimple PROPERTY CXX_STANDARD 11)
  add_dependencies(TestSimple ${external_project_dependencies})

  
  add_executable(TestMF mf.cpp)
  target_link_libraries(TestMF csci5570)
  target_link_libraries(TestMF ${HUSKY_EXTERNAL_LIB})
  target_link_libraries(TestMF ${GTEST_LIBRARIES})
  target_link_libraries(TestMF ${GMOCK_LIBRARIES})
  set_property(TARGET TestMF PROPERTY CXX_STANDARD 11)
  add_dependencies(TestMF ${external_project_dependencies})
 
  add_executable(TestLR lr.cpp)
  target_link_libraries(TestLR csci5570)
  target_link_libraries(TestLR ${HUSKY_EXTERNAL_LIB})
  target_link_libraries(TestLR ${GTEST_LIBRARIES})
  target_link_libraries(TestLR ${GMOCK_LIBRARIES})
  set_property(TARGET TestLR PROPERTY CXX_STANDARD 11)
  add_dependencies(TestLR ${external_project_dependencies})
 


  
  
