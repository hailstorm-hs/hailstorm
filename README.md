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
    Hailstorm requires Kafka to be installed and operating. For basic set-up instructions, 
    see https://github.com/cosbynator/haskakafka.

    After Kafka is installed and running, you will also have to install the haskakafka bindings
    for Haskell. These aren't yet available on Cabal, outside of your sandbox run:
        
        cabal install cabalg 

    Inside the sandbox, run

        cabalg https://github.com/cosbynator/haskakafka.git 


And you are done!

## Authors
[Thomas Dimson](https://github.com/cosbynator/),
[Milind Ganjoo] (https://github.com/mganjoo/)
