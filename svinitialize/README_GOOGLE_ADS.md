# Singleview API Retriever Python Google Ads Initialize Procedures

This document describes how to initialize google ads API access for the sv-api-retriever.

## Prerequisites

Please make sure that you have installed Python 3.7 or higher and sv-api-retriever(https://github.com/singleview-co-kr/sv_API_retriever).

## Setup Authentication

This API uses OAuth 2.0. See [Using OAuth 2.0 to Access Google APIs](https://developers.google.com/identity/protocols/oauth2) to learn more.

To get started quickly, follow these steps.

1. Visit https://console.developers.google.com to register your google-ads application.
1. click [user auth] info
1. Click **+CREATE CREDENTIALS > OAuth client ID**.
1. Select **Desktop app** or **Web app**as the application type, give it a name, then click
   **Create**.
1. From the Credentials page, click **Download JSON** next to the client ID you just created and save the file as `client_secret_google_ads.json` in the conf directory.

## Set up your environment ##
### Via the command line ###

Execute the following command in the root directory of the sv-api-retriever to install the dependencies.

    $ pip install --upgrade google-api-python-client
    $ pip install --upgrade google-auth-oauthlib

## Running the Examples ##

Before proceeding with the following steps, make sure you are in the root directory of the svinitialize.

1. To acquire a refresh token, execute the following command.

        $ python generate_user_credentials -c ../conf/client_secret_google_ads.json

1. Complete the authorization steps in the browser.
1. Enter the refresh token in the terminal google suggested like

"Your refresh token is: **1//0ejdfgsVfw6PjtjMhg3msdfgiKkd5psfgWGeDrUtI**

Add your refresh token to your client library configuration as described here: https://developers.google.com/google-ads/api/docs/client-libs/python/configuration"

Enjoy exploring the sv_api_retriever!
