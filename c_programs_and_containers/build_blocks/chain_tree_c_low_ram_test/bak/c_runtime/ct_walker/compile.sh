#!/bin/bash
# Quick compile script for testing without using make

echo "Compiling CT_Tree_Walker..."

# Compile the library
gcc -Wall -Wextra -std=c99 -O2 -c CT_Tree_Walker.c -o CT_Tree_Walker.o

# Compile the example
gcc -Wall -Wextra -std=c99 -O2 -c example.c -o example.o

# Link
gcc CT_Tree_Walker.o example.o -o example

echo "Done! Run with: ./example"