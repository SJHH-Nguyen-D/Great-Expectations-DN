cp ./docker-entrypoint-initdb.d/pg_hba.conf /var/lib/postgresql/data/
# now you just have to bash in and run:
# psql -h localhost -U postgres
# SELECT pg_reload_conf();
# \q
# then login again


# su -s '/usr/lib//postgresql/14/bin/pg_ctl reload'
# su -c '/usr/lib//postgresql/14/bin/pg_ctl reload'
# su -s 'pg_ctl reload'