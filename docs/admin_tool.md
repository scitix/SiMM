# SiMM Admin Control Tool
Unified admin tool to operate maintanence works on SiMM Cluster, Nodes, Shards, Global Flags, KV entry. 

## Binary Output
```bash
# source code
tools/simm_ctl_admin.cc 

# binary output path after build
build/${build_mode}/bin/tools/simmctl
```


## Help Output
```bash
./simmctl -h
Usage: ./simmctl [OPTIONS] SUBCOMMAND [ARGS]

OPTIONS:
SiMMCtl - SiMM RPC Management Tool:
  -h [ --help ]              Show help message
  -i [ --ip ] arg            Target Cluster Manager IP address
  -p [ --port ] arg (=30002) Target Cluster Manager port
  -v [ --verbose ]           Enable verbose output

SUBCOMMANDS:
  node list [OPTIONS]           List all nodes
  node set <IP:PORT> <STATUS>   Set node status (0=DEAD, 1=RUNNING)
  shard list [OPTIONS]          List all shards
  gflag list [OPTIONS]          List all gflags
  gflag get <NAME> [OPTIONS]    Get a gflag value
  gflag set <NAME> <VALUE>      Set a gflag value
```

## Cheatsheet
```xml
simm-ctl -i <ip> -p <port=30002>
  ├── node
  │   ├── list [-v]
  │   └── set <ID> <STATUS> [-v]
  |
  ├── shard
  │   └── list [-v]
  |
  |── kv
  │   └── exist <key>
  │   └── get   <key>
  │   └── del   <key>
  |
  └── gflag
      ├── list [-v]
      ├── get <NAME> [-v]
      └── set <NAME> <VALUE> [-v]
```

## Command Examples
```
./simmctl [OPTIONS] TYPE SUBCOMMAND
```

### OPTIONS
- -i / --ip   (Required): Specify ClusterManager IP
- -p / --port（Optional, default is 30002): Specify ClusterManager Port to receive RPC requests
- -v / --verbose （Optional): Enable verbose output

### Node Subcommand
#### List DataServers list of cluster
```bash
./simmctl -i [IP] node list [-v]
```
example
```bash
./simmctl --ip=172.168.1.1 node list
```
output
```bash
+--------------------+------------+
| Node Address       | Status     |
+--------------------+------------+
| 172.18.77.65:40000 | RUNNING    |
+--------------------+------------+
| 172.18.11.43:40000 | DEAD       |
+--------------------+------------+
```

#### Set status of single DataServer
```bash
./simmctl -i [IP] node set [IP:PORT] [STATUS]
```
**STATUS** types include ***RUNNING***, ***DEAD***

⚠️ **CAUTION**: Set DataServer Status to ***DEAD*** may trigger background shards migration!

example
```bash
# notify ClusterManager to add DataServer(172.18.11.43) into blacklist

./simmctl --ip=172.168.1.1 node set 172.18.11.43:40002 DEAD
```

### Shard Subcommand
#### Query cluster shards distribution
```bash
./simmctl -i [IP] shard list [-v]
```
example
```bash
./simmctl --ip=172.168.1.1 shard list
```
output
```bash
+--------------------+---------------+
| Data Server        | Shard Count   |
+--------------------+---------------+
| 172.18.11.43:40000 | 8192          |
+--------------------+---------------+
| 172.18.77.65:40000 | 8192          |
+--------------------+---------------+
```
The command output means cluster has two data servers, and one half of shards loaded on 172.18.11.43 and another half of shards loaded on 172.18.77.65   
Use -v / --verbose to see complete shards distribution list (one shard per line)

### Global Flag Subcommand
#### Get complete global flags of single node
```bash
./simmctl -i [IP] gflag list [-v]
```
example
```bash
./simmctl --ip=172.168.1.1 --port=30002 gflag list
```
output
```bash
+------------------------------+------------------------------+
| Flag Name                    | VALUE                        |
+------------------------------+------------------------------+
| alsologtoemail               |                              |
+------------------------------+------------------------------+
...
+------------------------------+------------------------------+
| folly_hazptr_use_executor    | true                         |
+------------------------------+------------------------------+
```
Use -v/--verbose to display Key/Value/Default/Type/Description of global flags

#### Get single global flag of single node
```bash
/simmctl -i [IP] gflag get [FLAG] [-v]
```
example
```bash
# get CM heartbeat timeout(with DataServer) flag value

./simmctl --ip=172.168.1.1 --port=30002 gflag get cm_heartbeat_timeout_inSecs
```
output
```bash
+--------------------+--------------------------------------------------+
| Flag Name          | cm_heartbeat_timeout_inSecs                      |
+--------------------+--------------------------------------------------+
| VALUE              | 5                                                |
+--------------------+--------------------------------------------------+
```

#### Set single global flag value
```bash
./simmctl -i [IP] gflag set [FLAG] [VALUE]
```
example
```bash
# origin value of flag ：30
./simmctl -i 172.168.1.1 -p 30002 gflag get cm_heartbeat_timeout_inSecs
+--------------------+--------------------------------------------------+
| Flag Name          | cm_heartbeat_timeout_inSecs                      |
+--------------------+--------------------------------------------------+
| VALUE              | 30                                               |
+--------------------+--------------------------------------------------+

# Set flag value
./simmctl -i 172.168.1.1 -p 30002 gflag set cm_heartbeat_timeout_inSecs 10
+--------------------+--------------------------------------------------+
| Flag Name          | cm_heartbeat_timeout_inSecs                      |
+--------------------+--------------------------------------------------+
| VALUE              | 10                                               |
+--------------------+--------------------------------------------------+

# Get flag value for double check
./simmctl -i 172.168.1.1 -p 30002 gflag get cm_heartbeat_timeout_inSecs
+--------------------+--------------------------------------------------+
| Flag Name          | cm_heartbeat_timeout_inSecs                      |
+--------------------+--------------------------------------------------+
| VALUE              | 10                                               |
+--------------------+--------------------------------------------------+
```

### KV Subcommand
🚧

## Issues
If you find any issues about simmctl tool, feel free to create issue to report it, thx!
