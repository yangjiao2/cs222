## For students: change this path to the root of your code
#CODEROOT = "/home/user/cs222/codebase"
CODEROOT = $(shell dirname $(shell pwd))

#CC = gcc
CC = clang++

#CPPFLAGS = -Wall -I$(CODEROOT) -O3  # maximal optimization
CPPFLAGS = -Wall -I$(CODEROOT) -g     # with debugging info
