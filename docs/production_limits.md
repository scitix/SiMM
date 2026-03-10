## Release-v0.1.0

| No         | Limit Item | Limit Value |
| :---       | :----      | :----       |
| 1 | Key Length | 1B ~ 256 B |
| 2 | Value Length | 1 B ~ 67088384 B (64MiB - 20KiB) |
| 3 | SDK APIs | MDel/ AsyncMdel <font color=red>NOT</font> supported | 
| 4 | Eviction Policy | <font color=yellow>ONLY</font> FIFO policy supported |
| 5 | Network Env | <font color=yellow>ONLY</font> RDMA(IB/RoCEv2 supported), TCP/UDP <font color=red>NOT</font> supported |
| 6 | OS Version | <font color=yellow>ONLY</font> Ubuntu 22.04/24.04 supported |