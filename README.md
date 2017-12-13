# [Battleship](https://github.com/scoutsaachi/zatt)

Battleship is a Byzantine fault tolerant prototype of a distributed storage system built on the [Raft](https://raft.github.io/) consensus algorithm. This project was originally forked from 
[Zatt](https://github.com/simonacca/zatt)

By default, clients share a `dict` data structure, although every python object
is potentially replicable with the `pickle` state machine.

![Zatt Logo](docs/logo.jpg?raw=true "Zatt Logo")

Please note that Battleship servers and clients must both be run using python3.

## Structure of the project

The most relevant part of the code concerning BFT Raft is in the [states](https://github.com/scoutsaachi/zatt/blob/master/zatt/server/states.py) and in the [log](https://github.com/scoutsaachi/zatt/blob/master/zatt/server/log.py) files.

## How to run


### Spinning up a cluster of servers

A server can be configured with command-line options or with a config file,
in this example, we are going to use both.

First, create an empty folder and enter it:
`$ mkdir zatt_cluster && cd zatt_cluster`.

Now create a config file `config.json` with the following content:
```
{  
    "StorageDir":"zatt_cluster/storage",
    "keyDir":"zatt_cluster/keys",
    "cluster":[  
       [  
          "127.0.0.1",
          9110

       ],
       [  
          "127.0.0.1",
          9111
       ],
       [  
          "127.0.0.1",
          9112
       ],
       [  
          "127.0.0.1",
          9113
       ]
    ]
 }
```

You can now descend into the server directory and run the first node. 

`$ python3 -c ABSOLUTE_PATH_TO_BATTLESHIP_CONFIG  0`

This tells zattd to run the node with `id:0`, taking the info about address and port from the config file.

You can do the same to run the other three nodes, with ids 1, 2, and 3. 


### Client

To interact with the cluster, we need a client. Navigate down into the client directory and open a
 python interpreter (`$ python3`) and run the following commands:

```
In [1]: from distributedDict import createClientDict
In [2]: d = createClientDict('127.0.0.1', 9110, "ABSOLUTE_PATH_TO_CONFIG_FILE")
In [3]: d['key'] = 'value'
```

Let's retrieve `key1` from a second client:

Open the python interpreter on another terminal and run:

```
In [1]: from distributedDict import createClientDict
In [2]: d = createClientDict('127.0.0.1', 9111, "ABSOLUTE_PATH_TO_CONFIG_FILE")
In [3]: d['key']
Out[3]: 'value'
```

### Notes

Please note that you may need to remove the persistent storage for the node in order for
it to run properly. You can do so by going to the zatt_cluster directory and executing (`$ rm -rf storage`)

Also note that JSON, currently used for serialization, only supports keys of type `str` and values of type `int, float, str, bool, list, dict `.

## Tests
In order to run the tests:

* navigate to the test folder: `cd zatt/tests`
* execute: `python3 run.py`