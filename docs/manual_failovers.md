# Manual failover

While automatic failures are already baked-in, there may be times where a manually issued failover is necessary. The steps to perform a manual failover are listed below:

_**Note: The promotion candidate must reside within your PRIMARY_REGION.**_

**1. Connect to the Machine you wish to promote**
```
fly ssh console -s <app-name>
```

**2. Confirm the member is healthy**
```
# Switch to the postgres user and move to the home directory.
su postgres
cd ~

# Verify member is healthy
repmgr node check

Node "fdaa:0:2e26:a7b:7d17:9b36:6e4b:2":
	Server role: OK (node is standby)
	Replication lag: OK (0 seconds)
	WAL archiving: OK (0 pending archive ready files)
	Upstream connection: OK (node "fdaa:0:2e26:a7b:7d17:9b36:6e4b:2" (ID: 1375486377) is attached to expected upstream node "fdaa:0:2e26:a7b:c850:86cf:b175:2" (ID: 1177616922))
	Downstream servers: OK (this node has no downstream nodes)
	Replication slots: OK (node has no physical replication slots)
	Missing physical replication slots: OK (node has no missing physical replication slots)
	Configured data directory: OK (configured "data_directory" is "/data/postgresql")

```

**2. Stop the Machine running as primary**

Open up a separate terminal and stop the Machine running `primary`.

```
# Identify the primary
fly status --app <app-name>

ID            	STATE  	ROLE   	REGION	HEALTH CHECKS     	IMAGE                                           	CREATED             	UPDATED
6e8226ec711087	started	replica	lax   	3 total, 3 passing	davissp14/postgres-flex:recovery-fix-00 (custom)	2023-02-15T20:20:51Z	2023-02-15T20:21:10Z
6e82931b729087	started	primary	lax   	3 total, 3 passing	davissp14/postgres-flex:recovery-fix-00 (custom)	2023-02-15T20:19:58Z	2023-02-15T20:20:18Z
9185957f411683	started	replica	lax   	3 total, 3 passing	davissp14/postgres-flex:recovery-fix-00 (custom)	2023-02-15T20:20:24Z	2023-02-15T20:20:45Z


fly machines stop 6e82931b729087 --app <app-name>
```

**3. Run the standby promotion command**
Go back to the first terminal you opened that's connected to your promotion candidate.

```
# Issue a dry-run to ensure our candidate is eligible for promotion.
repmgr standby promote --siblings-follow --dry-run

INFO: node is a standby
INFO: no active primary server found in this replication cluster
INFO: all sibling nodes are reachable via SSH
INFO: 1 walsenders required, 10 available
INFO: 1 replication slots required, 10 available
INFO: node will be promoted using the "pg_promote()" function
INFO: prerequisites for executing STANDBY PROMOTE are met
```

**WARNING: It's important that you specify `--siblings-follow`, otherwise any other standbys will not be reconfigured to follow the new primary.**

If everything looks good, go ahead and re-run the command without the `--dry-run` argument.
```
repmgr standby promote --siblings-follow

NOTICE: promoting standby to primary
DETAIL: promoting server "fdaa:0:2e26:a7b:7d17:9b36:6e4b:2" (ID: 1375486377) using pg_promote()
NOTICE: waiting up to 60 seconds (parameter "promote_check_timeout") for promotion to complete
NOTICE: STANDBY PROMOTE successful
DETAIL: server "fdaa:0:2e26:a7b:7d17:9b36:6e4b:2" (ID: 1375486377) was successfully promoted to primary
INFO: executing notification command for event "standby_promote"
DETAIL: command is:
  /usr/local/bin/event_handler -node-id 1375486377 -event standby_promote -success 1 -details "server \"fdaa:0:2e26:a7b:7d17:9b36:6e4b:2\" (ID: 1375486377) was successfully promoted to primary" -new-node-id ''
NOTICE: executing STANDBY FOLLOW on 1 of 1 siblings
INFO: STANDBY FOLLOW successfully executed on all reachable sibling nodes
```

**4. Start the Machine that was previously operating as Primary**
```
fly machines start 6e82931b729087 --app <app-name>
```

The primary will come back up and recognizing that it's no longer the true primary and will rejoin the cluster as a standby.
