lib_LTLIBRARIES = libeasybd.la

AM_CPPFLAGS = -I$(top_srcdir)/include -I$(top_srcdir)/easyio/include

libeasybd_la_SOURCES = \
	client.hpp \
	client.cpp \
	capi.cpp

libeasybd_la_LIBADD = $(top_builddir)/easyio/src/libeasyio.la
libeasybd_la_LDFLAGS = -version-info 0:0:0

easybdincludedir = $(includedir)/easybd
easybdinclude_HEADERS = \
	$(top_srcdir)/include/easybd/client.h \
	$(top_srcdir)/include/easybd/protocol.h
