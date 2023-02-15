# Capacity monitoring

Disk capacity is monitored at regular intervals. When capacity exceeds the pre-defined threshold of 90%, every user-defined table will become read-only. When disk usage falls below the defined threshold, either through file cleanup or volume extension read/write will be re-enabled automatically.

## Resolving disk capacity issues
Disk capacity must be brought below 90% before read/writes will be re-enabled. The best way to do this is to simply extend your volume.

**List your volumes**
```
fly volumes list --app flex-testing

ID                  	STATE  	NAME   	SIZE	REGION	ZONE	ENCRYPTED	ATTACHED VM   	CREATED AT
vol_okgj54527584y2wz	created	pg_data	10GB	lax   	1581	true     	9185340f4d3383	59 minutes ago
```

**Extend the volume**
```
$ fly volumes extend vol_w0enxv3o9pov8okp --size 15 --app flex-testing
        ID: vol_okgj54527584y2wz
      Name: pg_data
       App: flex-testing
    Region: lax
      Zone: c6d5
   Size GB: 15
 Encrypted: true
Created at: 15 Feb 23 15:23 UTC

You will need to stop and start your machine to increase the size of the FS
```

**Restart the Machine tied to your Volume**
```
fly machines restart 9185340f4d3383 --app flex-testing
```


