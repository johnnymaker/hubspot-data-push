import requests,json
from config import hubspot_api_key, get_all_contacts_url
import urllib.parse 


headers = {
    'Authorization': f'Bearer {hubspot_api_key}',
    'Content-Type': 'application/json'
}

def get_all_contacts():
        
    max_results = 40000 
    count = 10000
    contact_list = []
    property_list = []
    parameter_dict = {'count': count}


    # Paginate your request using offset
    has_more = True
    while has_more:
        parameters = urllib.parse.urlencode(parameter_dict)  # Using the correct module
        get_url = get_all_contacts_url + "?" + parameters  # Adding '?' to separate the parameters in the URL
        r = requests.get(url=get_url, headers=headers)
        response_dict = json.loads(r.text)
        has_more = response_dict['has-more']
        contact_list.extend(response_dict['contacts'])
        parameter_dict['vidOffset'] = response_dict['vid-offset']
        if len(contact_list) >= max_results:  # Exit pagination, based on whatever value you've set your max results variable to.
            print('maximum number of results exceeded')
            break
    print('loop finished')

    list_length = len(contact_list)
    print(list_length)
    email_to_contact = {contact['identity-profiles'][0]['identities'][0]['value']: contact for contact in contact_list}
    return email_to_contact