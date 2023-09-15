import mysql.connector
import requests, json
from datetime import datetime, timezone
from config import mhost, mdatabase, mpassword, muser, hubspot_api_key, url


def update_customer_acc_info(email_to_contact):
    connection = mysql.connector.connect(
        host=mhost, user=muser, password=mpassword, database=mdatabase
    )

    headers = {
        "Authorization": f"Bearer {hubspot_api_key}",
        "Content-Type": "application/json",
    }

    try:
        if connection.is_connected():
            print("Connection MySQL database successful")

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
        DATE(bps.bill_period_end) AS billing_period_end,
        DATE(bps.bill_period_start) AS billing_period_start,
        DATE_ADD(DATE(bps.bill_period_end), INTERVAL 1 DAY) AS plan_renew_date,
        bp.points as total_billing_points,
        acc.name,
        DATE(bps.last_paid_plan_end_date) AS last_paid_plan_end_date
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
            MAX(CASE WHEN is_paid = 1 THEN ended_at END) AS last_paid_plan_end_date,
            MAX(started_at) AS bill_period_start,
            MAX(ended_at) AS bill_period_end
            FROM 
            bitquery.billing_periods 
            GROUP BY 
            account_id
) AS bps ON bp.account_id = bps.account_id 
        JOIN accounts acc ON acc.id = bp.account_id 
        ORDER BY 
        bp.account_id ASC
        """

        cursor.execute(query)

        results = cursor.fetchall()

        totalpayload = []

        it = 0
        for result in results:
            email = result[0]
            if email != "-" and email in email_to_contact:
                account_id = result[1]
                role = result[2]
                is_paying_customer = result[3]
                first_paid_plan_date = result[4]
                billing_period_end = result[5]
                billing_period_start = result[6]
                plan_renew_date = result[7]
                total_billing_points = result[8]
                acc_name = result[9]
                last_paid_plan_end_date = result[10]

                if acc_name:
                    name_parts = acc_name.split()
                    if len(name_parts) == 1:
                        firstname = name_parts[0]
                        lastname = ""
                    else:
                        firstname = name_parts[0]
                        lastname = " ".join(name_parts[1:])
                else:
                    firstname = ""
                    lastname = ""

                properties = [
                    {"property": "paying_customer", "value": is_paying_customer},
                    {"property": "Plan", "value": role},
                    {"property": "account_id", "value": account_id},
                    {"property": "total_billing_points", "value": total_billing_points},
                    {"property": "firstname", "value": firstname},
                    {"property": "lastname", "value": lastname},
                ]

                if first_paid_plan_date:
                    properties.append(
                        {
                            "property": "first_paid_plan_date",
                            "value": int(
                                datetime(
                                    first_paid_plan_date.year,
                                    first_paid_plan_date.month,
                                    first_paid_plan_date.day,
                                    tzinfo=timezone.utc,
                                ).timestamp()
                            )
                            * 1000,
                        }
                    )

                if billing_period_end:
                    properties.append(
                        {
                            "property": "billing_period_end",
                            "value": int(
                                datetime(
                                    billing_period_end.year,
                                    billing_period_end.month,
                                    billing_period_end.day,
                                    tzinfo=timezone.utc,
                                ).timestamp()
                            )
                            * 1000,
                        }
                    )

                if billing_period_start:
                    properties.append(
                        {
                            "property": "billing_period_start",
                            "value": int(
                                datetime(
                                    billing_period_start.year,
                                    billing_period_start.month,
                                    billing_period_start.day,
                                    tzinfo=timezone.utc,
                                ).timestamp()
                            )
                            * 1000,
                        }
                    )

                if plan_renew_date:
                    properties.append(
                        {
                            "property": "Plan_renew_date",
                            "value": int(
                                datetime(
                                    plan_renew_date.year,
                                    plan_renew_date.month,
                                    plan_renew_date.day,
                                    tzinfo=timezone.utc,
                                ).timestamp()
                            )
                            * 1000,
                        }
                    )

                if last_paid_plan_end_date:
                    properties.append(
                        {
                            "property": "last_paid_plan_end_date",
                            "value": int(
                                datetime(
                                    last_paid_plan_end_date.year,
                                    last_paid_plan_end_date.month,
                                    last_paid_plan_end_date.day,
                                    tzinfo=timezone.utc,
                                ).timestamp()
                            )
                            * 1000,
                        }
                    )

                contact_data = {"email": email, "properties": properties}

                totalpayload.append(contact_data)
                it += 1

        print("Total Contacts to be updated:", len(totalpayload))

        chunk_size = 8000
        for i in range(0, len(totalpayload), chunk_size):
            chunk_payload = totalpayload[i : i + chunk_size]
            batch_size = len(chunk_payload)
            payload = json.dumps(chunk_payload)
            response = requests.post(url, data=payload, headers=headers)
            print(
                f"Batch {i // chunk_size + 1} - Batch Size: {batch_size} - Status Code: {response.status_code}"
            )

    except Exception as e:
        print("Error connecting to MYSQL Database :", e)

    finally:
        cursor.close()
        connection.close()
