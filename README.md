# Standalone Postgres on Fly.io

This repo contains all the code and configuration necessary to run a [standalone Postgres instance](https://fly.io/docs/reference/postgres/) on Fly.io. This source is packaged into [Docker images](https://hub.docker.com/r/flyio/postgres-ha/tags) which allow you to track and upgrade versions cleanly as new features are added.

If you just want to get a standard Postgres standalone or highly-available setup on Fly, [check out the docs](https://fly.io/docs/reference/postgres/).
## Customizing Postgres behavior

Fly Postgres clusters are just regular Fly applications. If you need to customize Postgres in any way, you may fork this repo and deploy using normal Fly deployment procedures. You won't be able to use `fly postgres` commands with custom clusters. But it's a great way to experiment and potentially contribute back useful features!

Follow the rest of this README to run a customized setup.

## Prepare a new application

You'll need a fresh Fly application in your preferred region to get started. Run these commands within the fork of this repository.
### `fly launch --no-deploy`
This gets you started with a Fly application and an associated config file.
Choose `yes` when asked whether to copy the existing configuration to the newly generated app.

### Set secrets
This app requires a few secret environment variables. Generate a secure string for each, and save them.

`SU_PASSWORD` is the PostgreSQL super user password. The username is `flypgadmin`. Use these credentials to run high privilege administration tasks.

`OPERATOR_PASSWORD` is the password for the standard user `postgres`. Use these credentials for connecting from your application.

`fly secrets set SU_PASSWORD=<PASSWORD> OPERATOR_PASSWORD=<PASSWORD>`

### Create a volume

Create this volume in the region you specified in the previous step.

```
fly volumes create pg_data --region ord --size 10 # size in GB
```
### Deploy the app
```
fly deploy
fly status
```

## Connecting

Fly apps within the same organization can connect to your Postgres using the following URI:

```
postgres://postgres:<operator_password>@<postgres-app-name>.internal:5432/<database-name>
```

### Connecting to Postgres from your local machine

1. Setup WireGuard Tunnel ( If you haven’t already )
Follow the steps provided here: https://fly.io/docs/reference/private-networking/#step-by-step

2. Postgres needs to be installed on your local machine.

3. Use psql to connect to your Postgres instance.
```
psql postgres://postgres:<operator_password>@<postgres-app-name>.internal:5432
```


## Having trouble?

Create an issue or ask a question here: https://community.fly.io/


## Contributing
If you're looking to get involved, fork the project and send pull requests.
