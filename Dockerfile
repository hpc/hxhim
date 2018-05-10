FROM fedora:latest

ENV SWHOME=/install
ENV MPI_INSTALL=$SWHOME/mpich-3.2.1
ENV PATH=$PATH:$MPI_INSTALL/bin
ENV PKG_CONFIG_PATH=$SWHOME/cci/lib/pkgconfig:$SWHOME/argobots/lib/pkgconfig:$SWHOME/abt-snoozer/lib/pkgconfig:$SWHOME/mercury/lib/pkgconfig:$SWHOME/margo/lib/pkgconfig:$SWHOME/leveldb/lib64:$SWHOME/thallium/lib/pkgconfig:$MPI_INSTALL/lib/pkgconfig

ADD contrib/hxhim_dependencies.sh hxhim_dependencies.sh

RUN true \
&& echo fastestmirror=true >> /etc/dnf/dnf.conf \
&& dnf install -y yum-plugin-fastestmirror \
&& dnf install -y          \
       autoconf            \
       boost-devel         \
       clang               \
       cmake               \
       findutils           \
       git                 \
       libev-devel         \
       libtool             \
       libtool-ltdl-devel  \
       make                \
       perl-Math-BigInt    \
       tar                 \
       wget                \
# install MPI
&& export DL_DIR=/downloads \
&& mkdir -p $DL_DIR \
&& cd $DL_DIR \
&& wget http://www.mpich.org/static/downloads/3.2.1/mpich-3.2.1.tar.gz \
&& tar -xf mpich-3.2.1.tar.gz \
&& cd mpich-3.2.1 \
&& mkdir -p build \
&& cd build \
&& CC=clang CXX=clang++ ../configure --prefix=$MPI_INSTALL --enable-cxx --enable-threads=multiple --disable-fortran \
&& make -j $(nproc --all) \
&& make -j $(nproc --all) install \
&& cd / \
# install hxhim dependencies
&& chmod +x hxhim_dependencies.sh \
&& ./hxhim_dependencies.sh --BMI --SM $DL_DIR $SWHOME \
&& rm -rf $DL_DIR hxhim_dependencies.sh