configure_file(${CMAKE_CURRENT_SOURCE_DIR}/version.py.in
               ${CMAKE_CURRENT_SOURCE_DIR}/simm/version.py)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/setup.py.in
               ${CMAKE_CURRENT_SOURCE_DIR}/setup.py)
configure_file(${CMAKE_SOURCE_DIR}/README.md
               ${CMAKE_CURRENT_SOURCE_DIR}/README.md COPYONLY)
configure_file(${CMAKE_SOURCE_DIR}/LICENSE
               ${CMAKE_CURRENT_SOURCE_DIR}/LICENSE COPYONLY)

execute_process(
    COMMAND 
    "${Python_EXECUTABLE}" -m nanobind --cmake_dir OUTPUT_STRIP_TRAILING_WHITESPACE 
    OUTPUT_VARIABLE nanobind_DIR)
find_package(nanobind CONFIG REQUIRED)

nanobind_add_module(_kv simm/kv/ext.cpp)
target_link_libraries(_kv PRIVATE client_static)
set_target_properties(
    _kv
    PROPERTIES 
    LIBRARY_OUTPUT_DIRECTORY 
    ${CMAKE_CURRENT_SOURCE_DIR}/simm/kv
)
install(TARGETS _kv DESTINATION kv)

add_custom_target(py_simm ALL
                  COMMAND cd "${CMAKE_CURRENT_SOURCE_DIR}" && rm -rf build dist &&
                  "${Python_EXECUTABLE}" "${CMAKE_CURRENT_SOURCE_DIR}/setup.py" bdist_wheel &&
                  pip install "${CMAKE_CURRENT_SOURCE_DIR}/dist/*" --force-reinstall
                  DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/setup.py _kv)
