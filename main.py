from fetch_hubspot_contact import get_all_contacts
from customer_acc_info import update_customer_acc_info
from customer_api_use import update_customer_api_use
from points_consumed import update_points_consumed

if __name__ == "__main__":

    email_to_contact = get_all_contacts()  # call once
    print("________________________________________")

    print("Updating Customer Acc Info")
    update_customer_acc_info(email_to_contact) 
    
    print("-------")

    print("Updating Customer Api Usage")
    update_customer_api_use(email_to_contact)

    print("-------")

    print("Updating Customer Points")
    update_points_consumed(email_to_contact)
   
    print("-------")
    
    print("Done...")
