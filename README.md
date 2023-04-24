![GitHub release (latest by date)](https://img.shields.io/github/v/release/fly-apps/postgres-flex)
[![DeepSource](https://deepsource.io/gh/fly-apps/postgres-flex.svg/?label=active+issues&token=VOdkBvMAf90cLzNVB3k0WpJC)](https://deepsource.io/gh/fly-apps/postgres-flex/?ref=repository-badge)

# High Availability Postgres on Fly.io
This repo contains all the code and configuration necessary to run a [highly available Postgres cluster](https://fly.io/docs/postgres/) in a Fly.io organization's private network. This source is packaged into [Docker images](https://hub.docker.com/r/flyio/postgres-flex/tags) which allow you to track and upgrade versions cleanly as new features are added.


## Getting started
```bash
# Be sure you're running the latest version of flyctl.
fly version update

# Provision a 3 member cluster
fly pg create --name <app-name> --initial-cluster-size 3 --region ord --flex
```

## High Availability
For HA, it's recommended that you run at least 3 members within your primary region. Automatic failovers will only consider members residing within your primary region. The primary region is represented as an environment variable defined within the `fly.toml` file.

## Horizontal scaling
Use the clone command to scale up your cluster.
```
# List your active Machines
fly machines list --app <app-name>

# Clone a machine into a target region
fly machines clone <machine-id> --region <target-region>
```

## Staying up-to-date!
This project is in active development so it's important to stay current with the latest changes and bug fixes.

```
# Use the following command to verify you're on the latest version.
fly image show --app <app-name>

# Update your Machines to the latest version.
fly image update --app <app-name>

```

## TimescaleDB support
We currently maintain a separate TimescaleDB-enabled image that you can specify at provision time.

```
fly pg create  --image-ref flyio/postgres-flex-timescaledb:15
```

## Having trouble?
Create an issue or ask a question here: https://community.fly.io/

## Contributing
If you're looking to get involved, fork the project and send pull requests.
