FROM postgres:14.4
ENV POSTGRES_PASSWORD password 
COPY init.sql /docker-entrypoint-initdb.d/
COPY pg_hba.conf /docker-entrypoint-initdb.d/
# ENV CONNECTION_STRING=postgresql+psycopg2://ge_user:pG9tC4qI5lE3sB2e@localhost/expectations