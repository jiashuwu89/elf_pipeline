
FROM python:3.8-slim as sciproc-dev

RUN apt-get update \
        && apt-get install --no-install-recommends -y \
        curl gfortran make libncurses5-dev

RUN curl -OL https://spdf.sci.gsfc.nasa.gov/pub/software/cdf/dist/cdf36_4/linux/cdf36_4-dist-all.tar.gz \
      && tar xzf cdf36_4-dist-all.tar.gz \
      && cd cdf36_4-dist \
      && make OS=linux ENV=gnu CURSES=yes FORTRAN=no UCOPTIONS=-O2 SHARED=yes all \
      && make INSTALLDIR=/usr/local/cdf install

RUN pip install poetry