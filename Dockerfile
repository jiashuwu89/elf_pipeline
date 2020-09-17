FROM python:latest

# CDF
RUN pushd /tmp \
    && curl -OL https://spdf.sci.gsfc.nasa.gov/pub/software/cdf/dist/cdf36_4/linux/cdf36_4-dist-all.tar.gz \
    && tar xzf cdf36_4-dist-all.tar.gz \
    && pushd cdf36_4-dist \
    && make OS=linux ENV=gnu CURSES=yes FORTRAN=no UCOPTIONS=-O2 SHARED=yes all \
    && make INSTALLDIR=/usr/local/cdf install \
    && pushd +2

# gfortran
RUN apt-get update && apt-get upgrade
RUN apt-get install -y gfortran

CMD ["python3"]
