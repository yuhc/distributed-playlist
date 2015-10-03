# Three-Phase-Commit Synchronized Playlist #

## Requirement ##

master.jar and worker.jar should under the same directory. (This requirement could be deleted after ANT is written)

## Parameters ##

```
java -jar master.jar -[c|s] 
```

* `-s`: input the commands via `[s]tdin`
* `c`: input the commands via `data/[c]ommand`

The logs are saved as `logs/*.log`.


## Command List ##

* `cp <n>` createProcesses <number of processes>
* `k <p>` kill <process>
* `ka` killAll
* `kl` killLeader
* `r <p>` revive <process>
* `rl` reviveLast
* `ra` reviveAll
* `pm <p> <n>` partialMessage <process, number of messages>
* `rm <p>` resumeMessages <process>
* `ac` allClear
* `rnc <p>` rejectNextChange <process>
* `pp` printParameters, prints master status which can be used for debug
* `q` quit, terminates the program

