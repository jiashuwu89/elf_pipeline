
FROM python:3.8-slim as sciproc-dev

RUN apt-get update \
        && apt-get install --no-install-recommends -y \
        curl gfortran make libncurses5-dev iproute2

RUN curl -OL https://spdf.gsfc.nasa.gov/pub/software/cdf/dist/cdf38_1/linux/cdf38_1-dist-all.tar.gz \
      && tar xzf cdf38_1-dist-all.tar.gz \
      && cd cdf38_1-dist \
      && make OS=linux ENV=gnu CURSES=yes FORTRAN=no UCOPTIONS=-O2 SHARED=yes all \
      && make INSTALLDIR=/usr/local/cdf install

ENV PATH="/root/.local/bin:$PATH"

RUN curl -sSL https://install.python-poetry.org | python3 - \
    && poetry config virtualenvs.in-project true
