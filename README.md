# Standalone Postgres
Single node Postgres on Fly


## Getting started

1. Fork/Clone this repository.

2. Prepare your app
```
fly launch --no-deploy # Specify yes, when asked to copy its configuration to new app. 
```

3. Create a volume
```
fly volumes create pg_data --region <region> --size <volume-size-in-gb>
```

4. Set required credentials
```
 fly secrets set SU_PASSWORD=<superuser-password> OPERATOR_PASSWORD=<postgres-user-password>
```

5. Deploy the app
```
fly deploy .
```

## Connecting

Fly apps within the same organization can connect to your Postgres using the following URI:

```
postgres://<user>:<password>@<postgres-app-name>.internal:5432/<database-name>
```

### Connecting to Postgres from your local machine

1. Setup WireGuard Tunnel ( If you haven’t already )
Follow the steps provided here: https://fly.io/docs/reference/private-networking/#step-by-step

2. Postgres needs to be installed on your local machine.

3. Use psql to connect to your Postgres instance.
```
psql postgres://postgres:<password>@<postgres-app-name>.internal:5432
```


## Having trouble?

Create an issue or ask a question here: https://community.fly.io/


## Contributing
If you're looking to get involved, fork the project and send pull requests.
