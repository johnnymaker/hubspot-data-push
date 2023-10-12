import requests,json
from clickhouse_driver import Client
from config import chost, hubspot_api_key, url
from urllib.parse import urlparse



def get_base_path(url):
    """Extract base path from a URL, excluding any query parameters or fragments."""
    parsed = urlparse(url)
    return parsed.scheme + "://" + parsed.netloc


def update_customer_api_use(email_to_contact):

  headers = {
      'Authorization': f'Bearer {hubspot_api_key}',
      'Content-Type': 'application/json'
  }
 
  try:
    
      client = Client(host=chost)
      print("Connection clickhouse database successful")
      
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
        ) AS api_calls_last_30_days,
        groupArray(DISTINCT COALESCE(nullIf(tags[2],''), tags[1])) AS blockchains,
        groupArray(DISTINCT tags[3]) AS apis,
        groupArray(DISTINCT referer_header) AS referrers
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

      results = client.execute(query)

      # list to store the fetched data 
      contacts_payload = []

      blockchain_accepted = ['ethereum', 'bsc', 'ethereum2' 'bitcoin', 'filecoin', 'eos', 'conflux', 'conflux_tethys', 'solana', 'bitcash', 'zcash', 'tron', 'litecoin', 'matic', 'algorand', 'dash', 'avalanche', 'stellar', 'bitcoinsv', 'klaytn', 'celo_rc1', 'goerli', 'search', 'ethclassic', 'celo_alfajores', 'binance', 'bsc_testnet', 'cosmos','terra', 'cardano' , 'fantom',  'dogecoin', 'velas', 'algorand_testnet' , 'celo_baklava' , 'velas_testnet', 'algorand_betanet', 'ethclassic_reorg' , 'hedera', 'eth2', 'elrond', 'moonbeam', 'conflux_hydra', 'ethpow', 'cronos', 'conflux_oceanus', 'medalla', 'crypto_testnet', 'ethclassic', 'flow', 'diem','diem_testnet', 'everscale', 'tezos', 'cosmoshub', 'heimdall', 'crypto_mainnet', 'ripple', 'ltc', 'BNB', 'polygon', 'doge', 'tronscan' , 'harmony', 'diem_testnet', 'diem', 'libra_testnet']
      api_accepted = ['dexTrades', 'balances', 'transfers', 'coinpath', 'transactions', 'smartContractCalls', 'omniTransactions','events']
      excluded_urls = ['https://ide.bitquery.io', 'https://graphql.bitquery.io', '://']

      for result in results:
          email, account_id, days_since_last_api_call, api_calls_last_7_days, api_calls_last_30_days, blockchains, apis, referrers = result
          if email != '-' and email in email_to_contact:
              blockchains_filtered = [item.capitalize() for item in blockchains if item in blockchain_accepted]
              api_filtered = [item.capitalize() for item in apis if item in api_accepted]
              blockchains_str = ' , '.join(blockchains_filtered)
              apis_str = ' , '.join(api_filtered)
              trimmed_referrers = [get_base_path(ref_url) for ref_url in referrers]
              unique_referrers = list(set(trimmed_referrers))
              filtered_referrers = [url for url in unique_referrers if url not in excluded_urls]
              referrers_str = ' , '.join(filtered_referrers)


              contact_data = {
                  "email": email,
                  "properties": [
                      {
                          "property": "days_since_last_api_call",
                          "value": days_since_last_api_call
                      },
                      {
                          "property": "api_calls_last_30_days",
                          "value": api_calls_last_30_days
                      },
                      {
                          "property": "api_calls_last_7_days",
                          "value": api_calls_last_7_days
                      },
                      {
                          "property": "account_id",
                          "value": account_id
                      },
                      {    "property": "used_blockchains",
                          "value": blockchains_str
                      },
                      {
                          "property": "used_apis",
                          "value": apis_str
                      },
                      {
                          "property": "referrers",
                          "value": referrers_str
                      }
                  ]
              }
              contacts_payload.append(contact_data)
            
      print("Total Contacts to be updated: ", len(contacts_payload))

      # Split data into batch of 10k contacts ( atmost 20k contacts can be updated in single request )
      chunk_size = 8000
      for i in range(0, len(contacts_payload), chunk_size):
          chunk_payload = contacts_payload[i:i + chunk_size]
          batch_size = len(chunk_payload)  
          payload = json.dumps(chunk_payload)
          response = requests.post(url, data=payload, headers=headers)
          print(f"Batch {i // chunk_size + 1} - Batch Size: {batch_size} - Status Code: {response.status_code}")

  except Exception as e:
      print("Error connecting to ClickHouse: ", e)

  finally:
      client.disconnect()
