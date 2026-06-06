cmake_minimum_required(VERSION 3.15)
project(crush-proxy LANGUAGES NONE)

find_program(GO_EXECUTABLE NAMES go REQUIRED)
if(NOT GO_EXECUTABLE)
  message(FATAL_ERROR "Go executable not found")
endif()

if(CMAKE_BINARY_DIR STREQUAL CMAKE_SOURCE_DIR)
  set(BUILD_ROOT "${CMAKE_SOURCE_DIR}/build")
else()
  set(BUILD_ROOT "${CMAKE_BINARY_DIR}")
endif()

set(CLIENT_SRC "${CMAKE_SOURCE_DIR}/client/main.go")
set(SERVER_SRC "${CMAKE_SOURCE_DIR}/server/main.go")

set(CLIENT_BIN "${BUILD_ROOT}/client/client")
set(SERVER_BIN "${BUILD_ROOT}/server/server")

set(CLIENT_CONF_SRC "${CMAKE_SOURCE_DIR}/client/client.conf")
set(SERVER_CONF_SRC "${CMAKE_SOURCE_DIR}/server/server.conf")

set(FULLCHAIN_SRC "${CMAKE_SOURCE_DIR}/server/fullchain.cer")
set(PRIVATE_KEY_SRC "${CMAKE_SOURCE_DIR}/server/private.key")

set(CLIENT_CONF_DST "${BUILD_ROOT}/client/client.conf")
set(SERVER_CONF_DST "${BUILD_ROOT}/server/server.conf")

set(FULLCHAIN_DST "${BUILD_ROOT}/server/fullchain.cer")
set(PRIVATE_KEY_DST "${BUILD_ROOT}/server/private.key")

file(GLOB CLIENT_GO_SOURCES "${CMAKE_SOURCE_DIR}/client/*.go")
file(GLOB SERVER_GO_SOURCES "${CMAKE_SOURCE_DIR}/server/*.go")

add_custom_command(
  OUTPUT "${CLIENT_CONF_DST}"
  COMMAND "${CMAKE_COMMAND}" -E make_directory "${BUILD_ROOT}/client"
  COMMAND "${CMAKE_COMMAND}" -E copy_if_different "${CLIENT_CONF_SRC}" "${CLIENT_CONF_DST}"
  DEPENDS "${CLIENT_CONF_SRC}"
  COMMENT "Copying client.conf to build directory"
)

add_custom_command(
  OUTPUT "${SERVER_CONF_DST}"
  COMMAND "${CMAKE_COMMAND}" -E make_directory "${BUILD_ROOT}/server"
  COMMAND "${CMAKE_COMMAND}" -E copy_if_different "${SERVER_CONF_SRC}" "${SERVER_CONF_DST}"
  DEPENDS "${SERVER_CONF_SRC}"
  COMMENT "Copying server.conf to build directory"
)

add_custom_command(
  OUTPUT "${FULLCHAIN_DST}"
  COMMAND "${CMAKE_COMMAND}" -E make_directory "${BUILD_ROOT}/server"
  COMMAND "${CMAKE_COMMAND}" -E copy_if_different "${FULLCHAIN_SRC}" "${FULLCHAIN_DST}"
  DEPENDS "${FULLCHAIN_SRC}"
  COMMENT "Copying fullchain.cer to build directory"
)

add_custom_command(
  OUTPUT "${PRIVATE_KEY_DST}"
  COMMAND "${CMAKE_COMMAND}" -E make_directory "${BUILD_ROOT}/server"
  COMMAND "${CMAKE_COMMAND}" -E copy_if_different "${PRIVATE_KEY_SRC}" "${PRIVATE_KEY_DST}"
  DEPENDS "${PRIVATE_KEY_SRC}"
  COMMENT "Copying private.key to build directory"
)

add_custom_command(
  OUTPUT "${CLIENT_BIN}"
  COMMAND "${CMAKE_COMMAND}" -E make_directory "${BUILD_ROOT}/client"
  COMMAND "${GO_EXECUTABLE}" build -o "${CLIENT_BIN}" ./client
  WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}"
  DEPENDS ${CLIENT_GO_SOURCES}
  COMMENT "Building client binary"
)

add_custom_command(
  OUTPUT "${SERVER_BIN}"
  COMMAND "${CMAKE_COMMAND}" -E make_directory "${BUILD_ROOT}/server"
  COMMAND "${GO_EXECUTABLE}" build -o "${SERVER_BIN}" ./server
  WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}"
  DEPENDS ${SERVER_GO_SOURCES}
  COMMENT "Building server binary"
)

add_custom_target(client ALL DEPENDS "${CLIENT_BIN}" "${CLIENT_CONF_DST}")
add_custom_target(server ALL DEPENDS "${SERVER_BIN}" "${SERVER_CONF_DST}" "${FULLCHAIN_DST}" "${PRIVATE_KEY_DST}")
