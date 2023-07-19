import mysql.connector
import requests,json
from datetime import datetime, timezone
from config import mhost, mdatabase, mpassword, muser, hubspot_api_key, url


connection = mysql.connector.connect(
    host = mhost,
    user = muser,
    password = mpassword,
    database = mdatabase
)

headers = {
    'Authorization': f'Bearer {hubspot_api_key}',
    'Content-Type': 'application/json'
}

try:

    if connection.is_connected():
       print("Connection to the database was successful.")

 
    cursor = connection.cursor()

   
    query = """SELECT 
    acc.email, 
    acc.id AS user_id,
    CASE WHEN acc.role = 'admin' THEN 'Admin' WHEN bp.points BETWEEN 0 
    AND 100000 THEN 'Developer' WHEN bp.points BETWEEN 100001 
    AND 3000000 THEN 'Team' WHEN bp.points BETWEEN 3000001 
    AND 10000000 THEN 'Startup' WHEN bp.points BETWEEN 10000001 
    AND 40000000 THEN 'Growth' WHEN bp.points BETWEEN 40000001 
    AND 100000000 THEN 'Business' ELSE 'Unknown' END AS plan, 
    IF(bp.is_paid = 1, 'true', 'false') AS is_paying_customer, 
    DATE(bps.became_a_customer_date) AS became_a_customer_date, 
    DATE(bps.plan_renew_date) AS plan_renew_date 
    FROM 
    (
        SELECT 
        account_id, 
        MAX(started_at) AS latest_started_at 
        FROM 
        bitquery.billing_periods 
        GROUP BY 
        account_id
    ) AS latest_bp 
    JOIN bitquery.billing_periods bp ON bp.account_id = latest_bp.account_id 
    AND bp.started_at = latest_bp.latest_started_at 
    LEFT JOIN (
        SELECT 
        account_id, 
        MIN(
            CASE WHEN is_paid = 1 THEN started_at END
        ) AS became_a_customer_date, 
        MAX(ended_at) AS plan_renew_date 
        FROM 
        bitquery.billing_periods 
        GROUP BY 
        account_id
    ) AS bps ON bp.account_id = bps.account_id 
    JOIN accounts acc ON acc.id = bp.account_id 
    ORDER BY 
    bp.account_id ASC 
    LIMIT 5
    """

    cursor.execute(query)

    
    results = cursor.fetchall()

    totalpayload = []

    for result in results:
                email = result[0]
                account_id = result[1]
                role = result[2]
                is_paying_customer = result[3]
                first_paid_plan_date = result[4]
                plan_renew_date = result[5]

                properties = [
                    {
                        "property": "paying_customer",
                        "value": is_paying_customer
                    },
                    {
                        "property": "Plan",
                        "value": role
                    },
                    {
                        "property": "account_id",
                        "value": account_id
                    }
                ]

                if first_paid_plan_date:
                    properties.append({
                        "property": "first_paid_plan_date",
                        "value": int(datetime(first_paid_plan_date.year, first_paid_plan_date.month, first_paid_plan_date.day, tzinfo=timezone.utc).timestamp()) * 1000
                    })

                if plan_renew_date:
                    properties.append({
                        "property": "Plan_renew_date",
                        "value": int(datetime(plan_renew_date.year, plan_renew_date.month, plan_renew_date.day, tzinfo=timezone.utc).timestamp()) * 1000
                    })

                contact_data = {
                    "email": email,
                    "properties": properties
                }

                totalpayload.append(contact_data)
    
    print("Total payload:", len(totalpayload))

    chunk_size = 50
    for i in range(0, len(totalpayload), chunk_size):
        chunk_payload = totalpayload[i:i + chunk_size]
        batch_size = len(chunk_payload)  
        payload = json.dumps(chunk_payload)
        response = requests.post(url, data=payload, headers=headers)
        print(f"Batch {i // chunk_size + 1} - Batch Size: {batch_size} - Status Code: {response.status_code}")


except Exception as e:
    print("Error connecting to ClickHouse:", e)

finally:
    cursor.close()
    connection.close()