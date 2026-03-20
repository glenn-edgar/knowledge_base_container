cmake_minimum_required(VERSION 3.16)
project(chaintree_viewer VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_AUTOMOC ON)

find_package(Qt6 REQUIRED COMPONENTS Widgets)

add_executable(chaintree_viewer
    src/main.cpp
    src/chaintree_model.h
    src/chaintree_model.cpp
    src/mainwindow.h
    src/mainwindow.cpp
)

target_link_libraries(chaintree_viewer PRIVATE Qt6::Widgets)
