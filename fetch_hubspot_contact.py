import requests,json
from config import hubspot_api_key, get_all_contacts_url
import urllib.parse 


headers = {
    'Authorization': f'Bearer {hubspot_api_key}',
    'Content-Type': 'application/json'
}

def get_all_contacts():

    print('Fetching contacts from Hubspot....')
        
    max_results = 50000 
    count = 10000
    contact_list = []
    property_list = []
    parameter_dict = {'count': count}


    # Paginate your request using offset
    has_more = True
    while has_more:
        parameters = urllib.parse.urlencode(parameter_dict) 
        get_url = get_all_contacts_url + "?" + parameters  # Adding '?' to separate the parameters in the URL
        r = requests.get(url=get_url, headers=headers)
        response_dict = json.loads(r.text)
        has_more = response_dict['has-more']
        contact_list.extend(response_dict['contacts'])
        parameter_dict['vidOffset'] = response_dict['vid-offset']
        if len(contact_list) >= max_results:  # Exit pagination, based on whatever value you've set your max results variable to.
            print('maximum number of results exceeded')
            break
    
    print('Total number of contacts fetched from Hubspot:',len(contact_list))
    
    email_to_contact = []

    for contact in contact_list:
            for profile in contact['identity-profiles']:
                for identity in profile['identities']:
                    if identity['type'] == "EMAIL" and identity.get('is-primary', False):
                        email_to_contact.append(identity['value'])

    print('Number of emails retrieved :',len(email_to_contact))
    
    return email_to_contact