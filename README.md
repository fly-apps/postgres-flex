# postgres-standalone
Standalone Postgres on Fly


## Getting started

1. Clone this repository

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
