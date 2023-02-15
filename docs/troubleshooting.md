# Troubleshooting


##  Member unregistration failed when removing machine

```
$ fly machines remove 9185340f4d3383 --app flex-testing
machine 9185340f4d3383 was found and is currently in stopped state, attempting to destroy...
unregistering postgres member 'fdaa:0:2e26:a7b:7d16:cff7:9849:2' from the cluster...  <insert-random-error-here> (failed)

9185340f4d3383 has been destroyed
```
Unfortionately, this can happen for a variety of reasons. If no action is taken, the member and associated replication slot will automatically be cleaned up after 24 hours.  Depending on the current cluster size, problems can arise if the down member impacts the clusters ability to meet quorum. If this case, it's important to take action right away to prevent your cluster from going read-only.


To address this, start by ssh'ing into one of your running Machines.

```
fly ssh console --app <app-name>
```

Switch to the postgres user and move into the home directory.
```
su postgres
cd ~
```

Use the `rempgr` cli tool to view the current cluster state.
```
repmgr daemon status

 ID | Name                             | Role    | Status        | Upstream                           | repmgrd | PID | Paused? | Upstream last seen
----+----------------------------------+---------+---------------+------------------------------------+---------+-----+---------+--------------------
 376084936 | fdaa:0:2e26:a7b:7d18:1a68:804e:2 | primary | * running     |                                    | running | 630 | no      | n/a
 1349952263 | fdaa:0:2e26:a7b:7d17:4463:955d:2 | standby | ? unreachable | ? fdaa:0:2e26:a7b:7d18:1a68:804e:2 | n/a     | n/a | n/a     | n/a
 1412735685 | fdaa:0:2e26:a7b:c850:8f12:fb1d:2 | standby |   running     | fdaa:0:2e26:a7b:7d18:1a68:804e:2   | running | 617 | no      | 1 second(s) ago
```

Manually unregister the unreachable standby.
```
repmgr standby unregister --node-id 1349952263
```
