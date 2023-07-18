import requests
import json
from clickhouse_driver import Client
from config import host, hubspot_api_key

url = "https://api.hubapi.com/contacts/v1/contact/batch"

headers = {
    'Authorization': f'Bearer {hubspot_api_key}',
    'Content-Type': 'application/json'
}

try:
    # Establish a connection
    client = Client(host=host)
    print("Connection to the database successful")

    # New ClickHouse query
    query = """
    SELECT 
      dictGetString(
        'graphql_accounts', 
        'email', 
        toUInt64(user_id)
      ) AS `email`, 
      user_id, 
      dateDiff('day', toDate(MAX(date)), today()) AS days_since_last_api_call,
       countDistinctIf(
        graphql_query_id,
        date >= today() - 6 
        AND date <= today()
      ) AS api_calls_last_7_days, 
      countDistinctIf(
        graphql_query_id,
        date >= today() - 29 
        AND date <= today()
      ) AS api_calls_last_30_days 
    FROM 
      (
        SELECT 
          * 
        from 
          graphql.user_requests_storage 
        UNION ALL 
        SELECT 
          * 
        from 
          graphql_old.user_requests_storage
      ) AS combined_data
    GROUP BY 
      user_id
    ORDER BY user_id asc
    """

    print("Fetching data from database...")
    results = client.execute(query)
    print("number of accounts: ", len(results))

    # Create a list to store the payload for updating multiple contacts
    contacts_payload = []

    for result in results:
        email, account_id, days_since_last_api_call, api_calls_last_7_days, api_calls_last_30_days = result
        if email != '-':
            contact_data = {
                "email": email,
                "properties": [
                    {
                        "property": "number_of_days_last_api_call_made",
                        "value": days_since_last_api_call
                    },
                    {
                        "property": "last_30_days_api_calls",
                        "value": api_calls_last_30_days
                    },
                    {
                        "property": "last_7_days_api_calls",
                        "value": api_calls_last_7_days
                    },
                    {
                        "property": "account_id",
                        "value": account_id
                    }
                ]
            }
            contacts_payload.append(contact_data)
          
    print("accounts to update: ", len(contacts_payload))

    # Split contacts_payload into batch of 15k contacts because atmost 20k contacts can be updated in single request
    chunk_size = 10000
    for i in range(0, len(contacts_payload), chunk_size):
        chunk_payload = contacts_payload[i:i + chunk_size]
        batch_size = len(chunk_payload)  
        payload = json.dumps(chunk_payload)
        response = requests.post(url, data=payload, headers=headers)
        print(f"Batch {i // chunk_size + 1} - Batch Size: {batch_size} - Status Code: {response.status_code}")

except Exception as e:
    print("Error connecting to ClickHouse: ", e)

finally:
    # Close the connection 
    client.disconnect()
