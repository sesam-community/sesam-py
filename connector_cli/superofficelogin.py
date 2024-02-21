import requests
import os
import json
import base64
import datetime
from Crypto.PublicKey import RSA
from Crypto.Util import number
from jwt import JWT, jwk_from_dict
from OpenSSL import crypto
from xml.dom import minidom


def get_long_int(nodelist):
    # Converts contents of element to long int
    rc = []
    for node in nodelist:
        if node.nodeType == node.TEXT_NODE:
            rc.append(node.data)
    string = ''.join(rc)
    return number.bytes_to_long(base64.b64decode(string))


def get_rsa_as_pem_content(private_key_xml):
    # Returns a PEM from Private RSA Key XML
    rsa_key_value = minidom.parseString(private_key_xml)
    modulus = get_long_int(rsa_key_value.getElementsByTagName('Modulus')[0].childNodes)
    exponent = get_long_int(rsa_key_value.getElementsByTagName('Exponent')[0].childNodes)
    d = get_long_int(rsa_key_value.getElementsByTagName('D')[0].childNodes)
    p = get_long_int(rsa_key_value.getElementsByTagName('P')[0].childNodes)
    q = get_long_int(rsa_key_value.getElementsByTagName('Q')[0].childNodes)
    q_inv = get_long_int(rsa_key_value.getElementsByTagName('InverseQ')[0].childNodes)
    private_key = RSA.construct((modulus, exponent, d, p, q, q_inv), False)
    pem_key = private_key.exportKey()
    return pem_key.decode('utf-8')


def get_base_url(environment, customer):
    r = requests.get(f"https://{environment}.superoffice.com/api/state/{customer}")
    if r.status_code == 200:
        js = r.json()

        # We need the base_url on the form https://{env}.superoffice.com:{xyz}
        if 'v1' in js:
            return js['v1'].split(f'/{customer}')[0].split(customer)[0]

        elif 'Api' in js:
            return js['Api'].split(f'/{customer}')[0].split(customer)[0]

    else:
        print(f"Unable to retrieve API URL for '{customer}'. Response "
              f"returned {r.status_code}: {r.text}")
        return None


def get_and_verify_system_token(id_token, jwks_uri):
    # Get system user token included inside 'id_token' and verify it with provided JWK
    id_jwt = JWT()
    jwks = requests.get(jwks_uri).json()['keys']
    key = jwk_from_dict(jwks[0])

    decoded_jwt = id_jwt.decode(id_token, key)

    system_token_key = 'http://schemes.superoffice.net/identity/system_token'
    if system_token_key in decoded_jwt:
        system_token = decoded_jwt[system_token_key]
    else:
        print(f"Could not get a SuperOffice system token from received JWT. The provided "
              f"client ID and secret might be for a non-system user context application.")
        system_token = None

    return system_token


def get_so_ticket(data, secrets):
    # 1. Decodes JWT inside oauth_token['id_token'] to get a system token
    # 2. Signs the system token with the RSA private key (from application secrets)
    # 3. Get another JWT using the signed system token, decode it for the ticket
    # 4. This ticket is used for future API calls
    # Heavily inspired by https://github.com/SuperOffice/devnet-python-system-user/blob/master/SystemUserToken.py
    ticket = {}
    customer = data["access_token"].split(":")[1].split(".")[0]  # Cust41398
    rsa_private_key = os.environ.get('RSA_PRIVATE_KEY')
    id_token = data['id_token']
    # TODO: jwks_uri is not always the same, it should be retrieved from some source (manifest?)
    with open("manifest.json", "r") as f:
        manifest = json.load(f)
    jwks_uri = manifest.get("oauth2").get("jwks_uri")
    application_token = secrets['oauth_client_secret']
    system_user_token = get_and_verify_system_token(id_token, jwks_uri)
    if not system_user_token:
        # return empty ticket object, the missing 'so_ticket' is handled in /login_callback
        return ticket

    environment = jwks_uri.split('.')[0].split('//')[-1]

    private_key = get_rsa_as_pem_content(rsa_private_key)

    time_formatted = datetime.datetime.utcnow().strftime("%Y%m%d%H%M")
    system_token = system_user_token + '.' + time_formatted

    key = crypto.load_privatekey(crypto.FILETYPE_PEM, private_key)
    signature = crypto.sign(key, system_token, 'sha256')
    signed_system_token = system_token + "." + \
                          base64.b64encode(signature).decode('UTF-8')

    ticket['signed_so_token'] = signed_system_token  # we might need this for refreshing later

    post_data = {
        "SignedSystemToken": signed_system_token,
        "ApplicationToken": application_token,
        "ContextIdentifier": customer,
        "ReturnTokenType": "JWT"
    }
    headers = {
        'Content-Type': 'application/json;charset=UTF-8',
        "Accept": "application/json;charset=UTF-8"
    }

    r = requests.post(f'https://{environment}.superoffice.com/Login/api/PartnerSystemUser/Authenticate',
                      data=json.dumps(post_data),  # must be a string
                      headers=headers)
    r_json = r.json()
    if r_json.get('IsSuccessful'):
        ticket_jwt = JWT()
        jwt_token = r_json['Token']
        jwks_response = requests.get(jwks_uri)
        jwks = json.loads(jwks_response.text)
        verifying_key = jwk_from_dict(jwks['keys'][0])
        message_received = ticket_jwt.decode(jwt_token, verifying_key)
        so_ticket = str(message_received['http://schemes.superoffice.net/identity/ticket'])
        account_id = str(message_received['http://schemes.superoffice.net/identity/ctx'])
        ticket['timestamp'] = datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z"
        ticket['so_ticket'] = so_ticket

        base_url = get_base_url(environment, account_id)
        test_url = f'{base_url}/v1/User/currentPrincipal'
        headers = {
            'Authorization': f"SOTicket {ticket['so_ticket']}",
            "SO-AppToken": secrets["oauth_client_secret"],
            "Accept": "application/json;charset=UTF-8"
        }
        r = requests.get(test_url, headers=headers)
        if r.status_code != 200:
            is_failed = True
            print(f"Failed to get user information from SuperOffice: {r.text}")
    else:
        ticket = None
        base_url = None
        print(f"Error retrieving SuperOffice ticket: {str(r_json.get('ErrorMessage'))}")

    # return ticket, account_id, environment
    return ticket, account_id, base_url
