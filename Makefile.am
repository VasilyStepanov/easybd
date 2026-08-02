ACLOCAL_AMFLAGS = -I m4

SUBDIRS = easyio client server

pkgconfigdir = $(libdir)/pkgconfig
pkgconfig_DATA = easybd.pc

test:
	$(MAKE) -C easyio test
