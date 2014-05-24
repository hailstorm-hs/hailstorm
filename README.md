# Hailstorm

**CS240h final project, Spring 2014**

## Reference
Storm tutorial: http://storm.incubator.apache.org/documentation/Tutorial.html

Exactly-once processing: https://github.com/jasonjckn/essays/blob/master/exactly_once_semantics.md

## Dependencies
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

And you are done!

## Authors
[Thomas Dimson](https://github.com/cosbynator/),
[Milind Ganjoo] (https://github.com/mganjoo/)
