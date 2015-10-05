# Three-Phase-Commit Synchronized Playlist #

## Requirement ##

* JDK 1.7
* Apache ANT

## How to Run

```bash
$ ant
$ cd out
$ java -jar master.jar
```

## Files and Logs ##

* Input (initial) playlist of process `i` should be stored in `data/playlist_init_i.txt`
* DT Log of process `i` will be stored in `log/dt_i.log`
* When `pl` is input to MASTER, the current playlists of available processes will be stored in `log/playlist_i.txt`

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
* `add <songName> <songURL>` add <songName> <songURL>
* `a` alias of `add`
* `edit <songName> <newSongName> <newSongURL>` edit <songName> <newSongName> <newSongURL>
* `e` alias of `edit`
* `remove <songName>` remove <songName>
* `rmv` alias of `remove`
* `pp` printParameters, prints master status which can be used for debug
* `pl` printList, prints current playlists of available workers to storage
* `q` quit, terminates the program

## Messages between Workers ##

### Format ###
"sender_id instruction parameter1 parameter2 ..."

### Examples ###

MASTER is always the process 0. Assume the COORDINATOR is 1 and PARTICIPANT is 2.

* VOTE_REQ: `1 vr add songName URL`
* VOTE: `2 v yes` or `2 v no`
* ABORT: `2 abt`
* RECOMMIT: `1 rc`
* ACK: `2 ack`
* COMMIT: `1 c`
* ADD: `1 add songName URL`
* REMOVE: `1 rm songName`
* EDIT: `1 e songName newSongName newURL`
* STATE_REQ: `2 sr`
* STATE_ACK: `1 sa`

