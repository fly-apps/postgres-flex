
# High Availability Postgres on Fly.io
This repo contains all the code and configuration necessary to run a [highly available Postgres cluster](https://fly.io/docs/postgres/) in a Fly.io organization's private network. This source is packaged into [Docker images](https://hub.docker.com/r/flyio/postgres-flex/tags) which allow you to track and upgrade versions cleanly as new features are added.


## Pre-release
This project is currently a pre-release and should not yet be used for production use.

To install the pre-release cli, run the following:
```
curl -L https://fly.io/install.sh | sh -s -- prerelease
```

## Getting started

To get started, run the following:
```bash
fly pg create --name <app-name> --initial-cluster-size 3 --repmgr 
```


## Horizontal scaling
To scale up you're cluster, you can use the clone command:
```
# List your active Machines
fly machines list --app <app-name>

# Clone a machine into a target region
fly machines clone <machine-id> --region <target-region>
```

**For HA setups, it is recommended to maintain an odd number of members within your primary region!**


## Staying up-to-date!
This project is in active development so it's important to stay current with the latest changes and bug fixes. 

```
# Use the following command to verify you're on the latest version.
fly image show --app <app-name>

# Update your Machines to the latest version.
fly image update --app <app-name>

```

## Having trouble?
Create an issue or ask a question here: https://community.fly.io/

## Contributing
If you're looking to get involved, fork the project and send pull requests.

