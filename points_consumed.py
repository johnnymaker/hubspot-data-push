import mysql.connector
import requests
import json
from datetime import datetime, timedelta, timezone
from clickhouse_driver import Client
from config import mhost, mdatabase, mpassword, muser, hubspot_api_key, url, chost


def update_points_consumed(email_to_contact):

    headers = {
        'Authorization': f'Bearer {hubspot_api_key}',
        'Content-Type': 'application/json'
    }
    
    connection = mysql.connector.connect(
        host = mhost,
        user = muser,
        password = mpassword,
        database = mdatabase
    )

    def date_range(start_date, end_date):
        start_date = datetime.strptime(start_date, '%Y-%m-%d')
        end_date = datetime.strptime(end_date, '%Y-%m-%d')

        date_list = []
        while start_date <= end_date:
            date_list.append(datetime.strftime(start_date, '%Y-%m-%d'))
            start_date += timedelta(days=1)
        
        return date_list


    try:
        if connection.is_connected():
          print("Connection to the database was successful.")

        cursor = connection.cursor()

        query = """
        SELECT
    bp.account_id,
    a.email,
    sub.deactivated_at,
    MAX(bp.started_at) AS latest_bp_starts,
    MAX(bp.ended_at) AS latest_bp_end,
    p.channel AS payment_method
FROM
    bitquery.billing_periods bp
JOIN
    bitquery.accounts a ON bp.account_id = a.id
LEFT JOIN (
    SELECT s1.account_id, s1.deactivated_at
    FROM bitquery.subscriptions s1
    JOIN (
        SELECT account_id, MAX(created_at) AS max_created_at
        FROM bitquery.subscriptions
        GROUP BY account_id
    ) AS s2 ON s1.account_id = s2.account_id AND s1.created_at = s2.max_created_at
) AS sub ON bp.account_id = sub.account_id
LEFT JOIN (
    SELECT account_id, channel
    FROM bitquery.payments po
    WHERE status = 2 and completed_at is not NULL and starting_at <= CURDATE() and ending_at >= CURDATE()
) AS p ON bp.account_id = p.account_id
GROUP BY
    bp.account_id,
    a.email,
    sub.deactivated_at;
    """

        cursor.execute(query)
        billing_periods = cursor.fetchall()

        print("Fetched latest billing period..")

        client = Client(host=chost)

        totalpayload = []

        # Query for all points by user and date
        query = """
        SELECT user_id, date, sum(points)
        FROM graphql.requests_statistics 
        GROUP BY user_id, date
        """
        all_points = client.execute(query)

        
        print("Fetched consumed points of user...")

        # Convert all_points to a dict for easy lookup
        all_points_dict = {(user_id, datetime.strftime(date, '%Y-%m-%d')): points for user_id, date, points in all_points}

        ct = 0
        for bp in billing_periods:
            account_id = bp[0]
            email = bp[1]
            deactivated_at = bp[2]
            started_at = datetime.strftime(bp[3], '%Y-%m-%d')
            ended_at = datetime.strftime(bp[4], '%Y-%m-%d')
            payment_method = bp[5]

            if email != '-' and email in email_to_contact:
        
                # Use all_points_dict to get points consumed
                points_consumed = round(sum(all_points_dict.get((account_id, date), 0)
                                    for date in date_range(started_at, ended_at)))

                contact_data = {
                    "email": email,
                    "properties": [
                        {
                            "property": "points_consumed",
                            "value": points_consumed
                        },
                        {
                            "property": "payment_method",
                            "value": payment_method
                        }
                    ]
                }
                if deactivated_at:
                    contact_data["properties"].append({
                        "property": "subscription_cancelled",
                        "value": int(datetime(deactivated_at.year, deactivated_at.month, deactivated_at.day, tzinfo=timezone.utc).timestamp()) * 1000
                    })

                totalpayload.append(contact_data)


        print("Total Contacts to be updated:", len(totalpayload))

        client.disconnect()  

        chunk_size = 8000
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