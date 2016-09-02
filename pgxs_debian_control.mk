#
# produce a debian/control file from a debian/control.in
#
# In debian/rules, include /usr/share/postgresql-common/pgxs_debian_control.mk
#
# Author: Dimitri Fontaine <dfontaine@hi-media.com>
#
debian/control: debian/control.in debian/pgversions
	pg_buildext checkcontrol || true

# run check when clean is invoked
clean: debian/control
.PHONY: debian/control
