# hubspot-data-push
Push customer data to HubSpot

```bash

# Clone this repository
$ git clone https://github.com/OmkarAcharekar/hubspot-data-push.git
$ cd hubspot-data-push

# Install libraries
$ pip install mysql-connector-python
$ pip install clickhouse-driver

# Add database connections, Hubspot API Key in config.py

hubspot_api_key = "***************************"

Clickhouse database
chost = "***********************"

MySQL database
mhost = "***************"
muser = "******************"
mpassword = "*************"
mdatabase = "*************"

# Run main.py
$ python main.py

```

