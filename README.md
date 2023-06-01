# GravelDB
A RocksDB based distributed key-value store implementing the Raft consensus algorithm using gRPC.

## Folder Structure

- api - Contains proto message and server definitions.
- client - Contains a simple blocking client implementation to interact with the cluster of database servers.
- server - Contains Raft consensus protocol and Database service implementations using gRPC.

## Implementation

- [x] Leader Election
- [x] Log Replication
- [ ] Membership Changes
- [ ] Log Compaction

## Tested and Working Correctly

- Leader Election Process (tested with a cluster size of 5)
  - Single leader elected in a term.
  - Leader elected only after receiving majority votes.

- Node Failures / Restarts
  - Cluster is up and ready to serve until a majority of nodes are up and communicating.
  - Node rejoins the cluster after restarting in the same state it was in when it stopped working.

- Log Backfill
  - If a node rejoins the cluster after a long time, it catches up with the leader by committing all missing entries from the leader's log into its own log.

## Build and run

- The application.yml file in the project has a default 5 node cluster configuration.
- Build the jar using Gradle Build Tool by executing the following command in the project root dir

  ```
  gradle :server:shadowJar
  ```

- Run 5 node cluster by executing the following commands in the project root dir:

  ```
  java -jar server/build/libs/server-1.0-SNAPSHOT-all.jar 1 5001 2>&1 server1.log &
  java -jar server/build/libs/server-1.0-SNAPSHOT-all.jar 2 5002 2>&1 server2.log &
  java -jar server/build/libs/server-1.0-SNAPSHOT-all.jar 3 5003 2>&1 server3.log &
  java -jar server/build/libs/server-1.0-SNAPSHOT-all.jar 4 5004 2>&1 server4.log &
  java -jar server/build/libs/server-1.0-SNAPSHOT-all.jar 5 5005 2>&1 server5.log &
  ```
  
