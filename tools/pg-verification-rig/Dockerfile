# A PostgreSQL TARGET with every instrument this product reads, for verifying collectors against a real
# self-hosted server (#2622, #2623, #2625, #2629, #2630 were all found this way).
#
# The extensions come from the PGDG apt repository as PACKAGES. Building them from source is the obvious
# approach and the wrong one: it turns a two-minute image into a twenty-minute one and pins you to whatever
# compiler flags you guessed, when Debian already ships them built against this exact server.
ARG PG_MAJOR=17
FROM postgres:${PG_MAJOR}
ARG PG_MAJOR

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      postgresql-${PG_MAJOR}-pg-wait-sampling \
      postgresql-${PG_MAJOR}-pg-stat-kcache \
      postgresql-${PG_MAJOR}-pg-qualstats \
      postgresql-${PG_MAJOR}-hypopg \
 && rm -rf /var/lib/apt/lists/*
