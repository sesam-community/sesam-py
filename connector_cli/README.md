# Connector CLI

This tool is a companion tool to sesampy.

## Init
```commandline
$ mkdir hubspot-connector
$ cd hubspot-connector
$ connectorpy init
$ ls
manifest.json
```

## Expand and upload
```commandline
$ connectorpy expand
$ cd .expanded
$ ls
test-env.json
node-metadata.conf.json
pipes
systems
$ sesampy upload
```

## Development
Do your work with Management Studio and your dev node, e.g. add a system and add company and contact pipes.


### OAuth2 setup
If the system is using oauth2, you can use the following tool to add secrets to the system. The tool expects a system with the following secrets and environment variables:
```
{
  "_id": "{{@ system @}}",
  "type": "system:[..]",
  [..]
  "oauth2": {
    "access_token": "$SECRET(oauth_access_token)",
    "client_id": "$SECRET(oauth_client_id)",
    "client_secret": "$SECRET(oauth_client_secret)",
    "refresh_token": "$SECRET(oauth_refresh_token)",
    "token_url": {{@ token_url @}}
  }
  [..]
}
```
The connector manifest also need to include the following re-usable oauth2 properties:
```
{
  [..]
  "auth": "oauth2",
  "oauth2": {
    "scopes": ["account:*", "business:*", "customer:*", "invoice:*", "product:*", "sales_tax:*", "transaction:*", "user:*", "vendor:*"],
    "token_url": "https://api.waveapps.com/oauth2/token/",
    "login_url": "https://api.waveapps.com/oauth2/authorize/"
  }
  [..]
}
```
This is how the tool is used:
```commandline
$ oauth2login.py --client_secret ZziobpmZ0DWC[..] --client_id gZLMgMG1[..] --service_url https://datahub-a6a45974.sesam.cloud/api --service_jwt eyJ0eXAiOiJKV1QiLCJhb[..]]

This tool will add oauth2 system secrets and add token_url to the environment variables:
  Service API: https://datahub-a6a45974.sesam.cloud/api
  System id: xxxxxx

To continue open the following link in your browser:
  Link: https://api.waveapps.com/oauth2/authorize/?client_id=gZLMgMG[..]

 * Serving Flask app 'oauth2login' (lazy loading)
 * Environment: production
   WARNING: This is a development server. Do not use it in a production deployment.
   Use a production WSGI server instead.
 * Debug mode: off
 * Running on http://127.0.0.1:5010/ (Press CTRL+C to quit)
Updated secret: oauth_access_token
Updated secret: oauth_refresh_token
Updated secret: oauth_client_id
Updated secret: oauth_client_secret
Updated environment variables
```

## Download and collapse
```commandline
$ sesampy download
$ cd ..
$ connectorpy collapse
$ ls
manifest.json
templates
.expanded
$ ls templates
company.json
contact.json
system.json
```

## Re-use templates across datatypes
You can re-use templates across datatypes by using the same template on multiple datatypes. In order to do development, only the datatype that has a template that matches the name of the datatype is used during collapse. The other datatypes are silently ignored.

```commandline
$ cat manifest.json 
{
  "datatypes": {
    "customer": {
      "template": "templates/customer.json" # the customer datatype is used during collapse
    },
    "product": {
      "template": "templates/customer.json" # the product datatype is ignored during collapse
    }
  },
  "system-template": "templates/system.json"
}
```

To reflect changes from the datatype used during collapse onto the other datatypes in your development environment one currently has to perform the following steps:
```commandline
$ sesampy download
$ cd ..
$ connectorpy collapse
$ connectorpy expand
$ cd .expanded
$ sesampy upload
```
