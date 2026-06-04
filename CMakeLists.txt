cmake_minimum_required(VERSION 3.15)
project(crush-proxy LANGUAGES NONE)

find_program(GO_EXECUTABLE NAMES go REQUIRED)
if(NOT GO_EXECUTABLE)
  message(FATAL_ERROR "Go executable not found")
endif()

set(CLIENT_SRC "${CMAKE_SOURCE_DIR}/client/main.go")
set(SERVER_SRC "${CMAKE_SOURCE_DIR}/server/main.go")

set(CLIENT_BIN "${CMAKE_BINARY_DIR}/client/client")
set(SERVER_BIN "${CMAKE_BINARY_DIR}/server/server")

set(CLIENT_CONF_SRC "${CMAKE_SOURCE_DIR}/client/client.conf")
set(SERVER_CONF_SRC "${CMAKE_SOURCE_DIR}/server/server.conf")

set(CLIENT_CONF_DST "${CMAKE_BINARY_DIR}/client/client.conf")
set(SERVER_CONF_DST "${CMAKE_BINARY_DIR}/server/server.conf")

add_custom_command(
  OUTPUT "${CLIENT_BIN}"
  COMMAND "${CMAKE_COMMAND}" -E make_directory "${CMAKE_BINARY_DIR}/client"
  COMMAND "${GO_EXECUTABLE}" build -o "${CLIENT_BIN}" ./client
  WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}"
  DEPENDS
    "${CLIENT_SRC}"
    "${CMAKE_SOURCE_DIR}/client/config.go"
    "${CMAKE_SOURCE_DIR}/client/local_server.go"
    "${CMAKE_SOURCE_DIR}/client/schedule_maintainer.go"
    "${CMAKE_SOURCE_DIR}/client/task.go"
  COMMENT "Building client binary"
)

add_custom_command(
  OUTPUT "${SERVER_BIN}"
  COMMAND "${CMAKE_COMMAND}" -E make_directory "${CMAKE_BINARY_DIR}/server"
  COMMAND "${GO_EXECUTABLE}" build -o "${SERVER_BIN}" ./server
  WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}"
  DEPENDS
    "${SERVER_SRC}"
    "${CMAKE_SOURCE_DIR}/server/config.go"
  COMMENT "Building server binary"
)

add_custom_command(
  OUTPUT "${CLIENT_CONF_DST}"
  COMMAND "${CMAKE_COMMAND}" -E make_directory "${CMAKE_BINARY_DIR}/client"
  COMMAND "${CMAKE_COMMAND}" -E copy_if_different "${CLIENT_CONF_SRC}" "${CLIENT_CONF_DST}"
  DEPENDS "${CLIENT_CONF_SRC}"
  COMMENT "Copying client.conf"
)

add_custom_command(
  OUTPUT "${SERVER_CONF_DST}"
  COMMAND "${CMAKE_COMMAND}" -E make_directory "${CMAKE_BINARY_DIR}/server"
  COMMAND "${CMAKE_COMMAND}" -E copy_if_different "${SERVER_CONF_SRC}" "${SERVER_CONF_DST}"
  DEPENDS "${SERVER_CONF_SRC}"
  COMMENT "Copying server.conf"
)

add_custom_target(client ALL DEPENDS "${CLIENT_BIN}" "${CLIENT_CONF_DST}")
add_custom_target(server ALL DEPENDS "${SERVER_BIN}" "${SERVER_CONF_DST}")
