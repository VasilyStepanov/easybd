noinst_LTLIBRARIES = libeasyio.la

AM_CPPFLAGS = -I$(top_srcdir)/easyio/include

libeasyio_la_SOURCES = \
	queue.cpp \
	libc_queue.hpp \
	libc_queue.cpp \
	libc_recv_stream.hpp \
	libc_recv_stream.cpp \
	framed_reader.cpp \
	$(top_srcdir)/easyio/include/easyio/task.hpp \
	$(top_srcdir)/easyio/include/easyio/queue.hpp \
	$(top_srcdir)/easyio/include/easyio/recv_stream.hpp \
	$(top_srcdir)/easyio/include/easyio/framed_reader.hpp \
	$(top_srcdir)/easyio/include/easyio/mutex.hpp
# NOTE: automake commits to using libeasyio_la_CPPFLAGS/_LIBADD instead of
# AM_CPPFLAGS/global LIBADD in the compile/link rules as soon as either is
# assigned anywhere in this file, even inside an `if` block that may end up
# false -- so both must be defined unconditionally (defaulting to empty/
# AM_CPPFLAGS) rather than only inside the WITH_LIBURING branch below, or a
# --without-liburing build silently loses -I$(top_srcdir)/easyio/include.
libeasyio_la_CPPFLAGS = $(AM_CPPFLAGS)
libeasyio_la_LIBADD =

if WITH_LIBURING
libeasyio_la_SOURCES += \
	uring_queue.hpp \
	uring_queue.cpp \
	uring_recv_stream.hpp \
	uring_recv_stream.cpp
libeasyio_la_CPPFLAGS += $(LIBURING_CFLAGS) -DEASYIO_WITH_LIBURING
libeasyio_la_LIBADD += $(LIBURING_LIBS)
endif
