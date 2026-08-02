bin_PROGRAMS = easybd-server

AM_CPPFLAGS = -I$(top_srcdir)/include -I$(top_srcdir)/easyio/include
AM_CXXFLAGS = -pthread
AM_LDFLAGS = -pthread

easybd_server_SOURCES = \
	main.cpp \
	server.hpp \
	server.cpp \
	session.hpp \
	session.cpp \
	aligned_buffer.hpp

easybd_server_LDADD = $(top_builddir)/easyio/src/libeasyio.la
