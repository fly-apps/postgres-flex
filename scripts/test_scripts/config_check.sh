#!/bin/bash

APP_NAME=$1

echo "App: $APP_NAME"

# Resolve the machine ID associated with the primary
MACHINE_ID=$(fly status --app $APP_NAME | grep primary | awk '{print $1}')
if (( ${#MACHINE_ID} != 14 ));
then
  echo "Error: unable to resolve primary machine"
  exit 1
fi
echo "Primary Machine ID: $MACHINE_ID"

# Resolve IP associated with the primary
PRIMARY_IP=$(fly machines list --app $APP_NAME | grep $MACHINE_ID | awk '{print $7}')
echo "Primary IP: $PRIMARY_IP"

# Pull operator password from environment.
OPERATOR_PASSWORD=$(fly ssh console -A $PRIMARY_IP --app $APP_NAME -q -C "printenv OPERATOR_PASSWORD" | tr -d "\r\n")
echo ""

echo "**Verifying connection target**"
result=$(psql "postgres://postgres:$OPERATOR_PASSWORD@$APP_NAME.internal:5432" -c "SELECT inet_server_addr();" | sed -n "3 p" 2>&1)
if [ $PRIMARY_IP = $result ];
then
    echo "Connected to:$result (correct)"
else
    echo "Connected to:$result (incorrect)"
fi


echo ""
echo "**Verifying PGBouncer configuration**"
for ip in $(dig $APP_NAME.internal aaaa +short)
do
  result=$(fly ssh console -A $ip --app $APP_NAME -q -C "cat data/pgbouncer/pgbouncer.database.ini" | grep "host=")
  pgbouncer=$(echo $result | cut -d= -f3 | awk '{print $1}')
  if [ $pgbouncer != $PRIMARY_IP ];
  then
    echo "$ip -> $pgbouncer  (incorrect)"
  else
    echo "$ip -> $pgbouncer (correct)"
  fi
done


echo ""
echo "**Verifying Readonly configuration**"
for ip in $(dig $APP_NAME.internal aaaa +short)
do
  result=$(psql postgres://postgres:$OPERATOR_PASSWORD@[$ip]:5433 -c 'SELECT 'CREATE DATABASE configcheck WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'configcheck')' 2>&1)
  if [ "$ip" = "$PRIMARY_IP" ];
  then
    if [[ $result =~ "read-only" ]];
    then
        echo "Role: Primary - $ip -> $result (incorrect)"
    else
        echo "Role: Primary - $ip -> $result (correct)"
    fi
  else
    if [[ $result =~ "read-only" ]];
    then
        echo "Role: Standby - $ip -> $result (correct)"
    else
        echo "Role: Standby - $ip -> $result (incorrect)"
    fi
  fi
done