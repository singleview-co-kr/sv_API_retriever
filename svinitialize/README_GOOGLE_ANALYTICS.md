# Singleview API Retriever Python Google Analytics Initialize Procedures

This document describes how to initialize google analytics API access for the sv-api-retriever based on [Hello Analytics API](https://developers.google.com/analytics/devguides/config/mgmt/v3/quickstart/service-py).

If you prefer more visualized contents, please visit  [https://github.com/singleview-co-kr/sv_API_retriever/issues/83](https://github.com/singleview-co-kr/sv_API_retriever/issues/83).

## Prerequisites

Please make sure that you have installed Python 3.7 and sv-api-retriever(https://github.com/singleview-co-kr/sv_API_retriever).

## Step 1: Enable the Analytics API

To get started using Google Analytics API, you need to first [use the setup tool](https://console.developers.google.com/start/api?id=analytics&credential=client_key), which guides you through creating a project in the Google API Console, enabling the API, and creating credentials.

### To create a client ID for google analytics, follow these steps. ###

1. Visit https://console.developers.google.com to register your google-ads application.
1. Open the Service accounts page. If prompted, select a project.
1. Click **+ Create Service Account** or **+서비스 계정 만들기**, enter a name and description for the service account. You can use the default service account ID, or choose a different, unique one. When done click **Create**.
1. The Service account permissions (optional) section that follows is not required. Click Continue.
1. On service account screen, click key menu on the top. Click **Add key** or **키 추가** button. Click **Create new key** or **새 키 만들기**
1. In the panel that appears, select the format for your key: JSON is recommended.
1. Click **Create** or **만들기**. Your new public/private key pair is generated and downloaded to your machine; it serves as the only copy of this key. For information on how to store it securely, see [Managing service account keys](https://cloud.google.com/iam/docs/understanding-service-accounts#managing_service_account_keys).
1. Click Close on the Private key saved to your computer dialog, then click Done to return to the table of your service accounts.
1. Move the generated key file to **conf** directory and rename it as **private_key_google_analytics.json**.

## Set up your environment ##
### Via the command line ###

Execute the following command in the root directory of the sv-api-retriever to install the dependencies.

    $ pip install --upgrade google-api-python-client

Enjoy exploring the sv_api_retriever!