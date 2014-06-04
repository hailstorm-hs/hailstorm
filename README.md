# Hailstorm

Hailstorm is a distributed stream computation system that uses exactly once
semantics.

Written by Thomas Dimson ([@cosbynator](https://github.com/cosbynator/)) and
Milind Ganjoo ([@mganjoo](https://github.com/mganjoo/)).

## References

The architecture of Hailstorm is based on [Apache
Storm](http://storm.incubator.apache.org/) (which is also the inspiration for
the name).

The exactly-one semantics implemented in Hailstorm are based on a high-level
description in an
[essay](https://github.com/jasonjckn/essays/blob/master/exactly_once_semantics.md)
by [@jasonjckn](https://github.com/jasonjckn).

## Dependencies

### Zookeeper

Hailstorm requires [Apache Zookeeper](http://zookeeper.apache.org/), its C
bindings, and its Haskell bindings
[`hzk`](https://hackage.haskell.org/package/hzk-1.0.0) to run.

On OSX, the `zookeeper` package on [homebrew](http://brew.sh/) contains the
binaries and C bindings for Zookeeper. You can install it as follows:

     brew install --c zookeeper

On Ubuntu, we recommend following the [official
instructions](http://zookeeper.apache.org/doc/trunk/zookeeperStarted.html#sc_Download)
to obtain and set up the Zookeeper binaries. To install the C bindings:

     sudo apt-get install libzookeeper-mt-dev

Finally, to build and install `hzk` on Mac OS X, run the following command in
your cabal sandbox:

     cabal install --extra-include-dirs=/usr/local/include/zookeeper hzk

The above command is required on Mac OS X because of the non-standard include
directory location. On Ubuntu, `cabal install hzk` should work.

### Kafka

Hailstorm requires [Apache Kafka](http://kafka.apache.org/) to be installed and
operating. See the [official
instructions](http://kafka.apache.org/documentation.html#quickstart) for
details.

### Haskakafka

Hailstorm uses
[Haskakafka](https://github.com/cosbynator/haskakafka), the Haskell bindings
written by our very own [@cosbynator](https://github.com/cosbynator/). Haskakfka, in
turn, depends on `librdkafka` (see the Haskakafka project page for installation
instructions).

Haskakafka itself is not available yet on Cabal, so install
[`cabalg`](http://hackage.haskell.org/package/cabalg) and
[`c2hs`](https://hackage.haskell.org/package/c2hs) into your sandbox:

    cabal install c2hs
    cabal install cabalg
    .cabal-sandbox/bin/cabalg https://github.com/cosbynator/haskakafka.git

On OS X, you may get `'stdio.h'` errors, in which case you should try:

    .cabal-sandbox/bin/cabalg https://github.com/cosbynator/haskakafka.git -- --with-gcc=gcc-4.8

And you are done!

## Running

First, start an instance of Zookeeper (`zkServer start` on Mac or `zkServer.sh
start` on Ubuntu).

Before running Hailstorm for the first time, you will have to initialize your
topology using the `zk_init` subcommand:

     hailstorm zk_init

Finally, run a sample topology:

     hailstorm -f data/test.txt run_sample

While it is running, you can extract debug metadata by executing

     hailstorm zk_show

