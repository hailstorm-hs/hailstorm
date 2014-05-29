# Hailstorm

**CS240h final project, Spring 2014**

## Reference
Storm tutorial: http://storm.incubator.apache.org/documentation/Tutorial.html

Exactly-once processing: https://github.com/jasonjckn/essays/blob/master/exactly_once_semantics.md

## Dependencies

### Zookeeper
Hailstorm requires Apache Zookeeper to run. On OSX, use homebrew and then install hzk

     brew install --c zookeeper
     cabal install --extra-include-dirs=/usr/local/include/zookeeper hzk
     zkServer start


Afterwards, you will have to initialize your topology using the zkinit function:

     hailstorm zk_init

Finally, run a sample topology

     hailstorm run_sample

While it is running, you can extract debug metadata by executing

     hailstorm zk_show

### Haskakafka
Hailstorm requires Kafka to be installed and operating and the librdkafka library 
installed. See https://github.com/cosbynator/haskakafka for details.

After you have installed librdkafka will have to install the haskakafka bindings
into your sandbox. These aren't yet available on Cabal, so install cabalg and c2hs 
in your sandbox:
    
    cabal install c2hs
    cabal install cabalg 
    .cabal-sandbox/bin/cabalg https://github.com/cosbynator/haskakafka.git

If you are getting `stdio.h' errors on OSX, try

    .cabal-sandbox/bin/cabalg https://github.com/cosbynator/haskakafka.git -- --with-gcc=gcc-4.8


And you are done!

## Authors
[Thomas Dimson](https://github.com/cosbynator/),
[Milind Ganjoo] (https://github.com/mganjoo/)
