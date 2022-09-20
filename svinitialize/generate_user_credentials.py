#!/usr/bin/env python
# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""This example will create an OAuth2 refresh token for the Google Ads API.

This example works with both web and desktop app OAuth client ID types.

https://console.cloud.google.com

IMPORTANT: For web app clients types, you must add "http://127.0.0.1" to the
"Authorized redirect URIs" list in your Google Cloud Console project before
running this example. Desktop app client types do not require the local
redirect to be explicitly configured in the console.

Once complete, download the credentials and save the file path so it can be
passed into this example.

This example is a very simple implementation, for a more detailed example see:
https://developers.google.com/identity/protocols/oauth2/web-server#python
"""

import argparse
import hashlib
import os
import re
import socket
import sys
from urllib.parse import unquote

# If using Web flow, the redirect URL must match exactly what’s configured in GCP for
# the OAuth client.  If using Desktop flow, the redirect must be a localhost URL and
# is not explicitly set in GCP.
from google_auth_oauthlib.flow import Flow

_SCOPE = "https://www.googleapis.com/auth/adwords"
_SERVER = "127.0.0.1"
_PORT = 8080
_REDIRECT_URI = f"http://{_SERVER}:{_PORT}"

# begin - singleview appending
_sRootPath = os.path.join(os.getcwd(), '..')
_sConfPath = os.path.join(_sRootPath, 'conf')
_sGoogleAdsYamlFileName = os.path.join(_sConfPath, 'google-ads.yaml')
# end - singleview appending


# this source from https://github.com/googleads/google-ads-python/blob/main/examples/authentication/generate_user_credentials.py
def sv_read_template_file():
    s_conf_templ = os.path.join(_sRootPath, 'svinitialize', 'templates', 'google-ads.yaml.template')
    s_template = ''
    with open(s_conf_templ, 'r', encoding='utf-8') as f:
        while True:
            s_line = f.readline()
            if not s_line:
                break
            s_template = s_template + s_line
    return s_template


def sv_user_input():
    # enquire configuration info
    s_developer_token = str(input("Google API developer token from https://console.developers.google.com: "))
    s_client_id = str(input("client id from client_secret.json: "))
    s_client_secret = str(input("client secret from client_secret.json: "))
    s_login_customer_id = str(input("customer ID of the authenticated manager account. (no dashes): ")).replace('-', '')
    return s_developer_token, s_client_id, s_client_secret, s_login_customer_id
# end - singleview appending


def main(client_secrets_path, scopes):
    """The main method, starts a basic server and initializes an auth request.

    Args:
        client_secrets_path: a path to where the client secrets JSON file is
          located on the machine running this example.
        scopes: a list of API scopes to include in the auth request, see:
            https://developers.google.com/identity/protocols/oauth2/scopes
    """
    # begin - singleview appending
    if not os.path.isdir(_sConfPath):
        try:
            os.makedirs(_sConfPath)  # check conf directory
        except OSError:
            print('Error: Creating directory. ' + _sConfPath)
    if os.path.isfile(_sGoogleAdsYamlFileName):
        print(_sGoogleAdsYamlFileName + " already exists, carefully remove old file if you want to continue")
        return
    tup_user_input = sv_user_input()
    # end - singleview appending

    flow = Flow.from_client_secrets_file(client_secrets_path, scopes=scopes)
    flow.redirect_uri = _REDIRECT_URI

    # Create an anti-forgery state token as described here:
    # https://developers.google.com/identity/protocols/OpenIDConnect#createxsrftoken
    passthrough_val = hashlib.sha256(os.urandom(1024)).hexdigest()

    authorization_url, state = flow.authorization_url(
        access_type="offline",
        state=passthrough_val,
        prompt="consent",
        include_granted_scopes="true",
    )

    # Prints the authorization URL so you can paste into your browser. In a
    # typical web application you would redirect the user to this URL, and they
    # would be redirected back to "redirect_url" provided earlier after
    # granting permission.
    print("Paste this URL into your browser: ")
    print(authorization_url)
    print(f"\nWaiting for authorization and callback to: {_REDIRECT_URI}")

    # Retrieves an authorization code by opening a socket to receive the
    # redirect request and parsing the query parameters set in the URL.
    code = unquote(get_authorization_code(passthrough_val))

    # Pass the code back into the OAuth module to get a refresh token.
    flow.fetch_token(code=code)
    refresh_token = flow.credentials.refresh_token

    print(f"\nYour refresh token is: {refresh_token}\n")
    print(
        "Add your refresh token to your client library configuration as "
        "described here: "
        "https://developers.google.com/google-ads/api/docs/client-libs/python/configuration"
    )
    # begin - singleview appending
    s_refresh_token = str(input("refresh token above: "))
    # write google-ads.yaml file.
    s_template = sv_read_template_file()
    s_google_ads_yaml_path = os.path.join(_sRootPath, 'conf', _sGoogleAdsYamlFileName)
    with open(s_google_ads_yaml_path, 'w', encoding='utf-8') as out:
        out.write(s_template.format(tup_user_input[0], tup_user_input[1], tup_user_input[2],
                                    s_refresh_token, tup_user_input[3]))
    sys.stdout.write(_sGoogleAdsYamlFileName + ' has been created successfully\n')
    # end - singleview appending


def get_authorization_code(passthrough_val):
    """Opens a socket to handle a single HTTP request containing auth tokens.

    Args:
        passthrough_val: an anti-forgery token used to verify the request
          received by the socket.

    Returns:
        a str access token from the Google Auth service.
    """
    # Open a socket at _SERVER:_PORT and listen for a request
    sock = socket.socket()
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((_SERVER, _PORT))
    sock.listen(1)
    connection, address = sock.accept()
    data = connection.recv(1024)
    # Parse the raw request to retrieve the URL query parameters.
    params = parse_raw_query_params(data)

    try:
        if not params.get("code"):
            # If no code is present in the query params then there will be an
            # error message with more details.
            error = params.get("error")
            message = f"Failed to retrieve authorization code. Error: {error}"
            raise ValueError(message)
        elif params.get("state") != passthrough_val:
            message = "State token does not match the expected state."
            raise ValueError(message)
        else:
            message = "Authorization code was successfully retrieved."
    except ValueError as error:
        print(error)
        sys.exit(1)
    finally:
        response = (
            "HTTP/1.1 200 OK\n"
            "Content-Type: text/html\n\n"
            f"<b>{message}</b>"
            "<p>Please check the console output.</p>\n"
        )

        connection.sendall(response.encode())
        connection.close()

    return params.get("code")


def parse_raw_query_params(data):
    """Parses a raw HTTP request to extract its query params as a dict.

    Note that this logic is likely irrelevant if you're building OAuth logic
    into a complete web application, where response parsing is handled by a
    framework.

    Args:
        data: raw request data as bytes.

    Returns:
        a dict of query parameter key value pairs.
    """
    # Decode the request into a utf-8 encoded string
    decoded = data.decode("utf-8")
    # Use a regular expression to extract the URL query parameters string
    match = re.search("GET\s\/\?(.*) ", decoded)
    params = match.group(1)
    # Split the parameters to isolate the key/value pairs
    pairs = [pair.split("=") for pair in params.split("&")]
    # Convert pairs to a dict to make it easy to access the values
    return {key: val for key, val in pairs}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "Generates OAuth2 refresh token using the Web application flow. "
            "To retrieve the necessary client_secrets JSON file, first "
            "generate OAuth 2.0 credentials of type Web application in the "
            "Google Cloud Console (https://console.cloud.google.com). "
            "Make sure 'http://_SERVER:_PORT' is included the list of "
            "'Authorized redirect URIs' for this client ID."
        ),
    )
    # The following argument(s) should be provided to run the example.
    parser.add_argument(
        "-c",
        "--client_secrets_path",
        required=True,
        type=str,
        help=(
            "Path to the client secrets JSON file from the Google Developers "
            "Console that contains your client ID, client secret, and "
            "redirect URIs."
        ),
    )
    parser.add_argument(
        "--additional_scopes",
        default=None,
        type=str,
        nargs="+",
        help="Additional scopes to apply when generating the refresh token.",
    )
    args = parser.parse_args()

    configured_scopes = [_SCOPE]

    if args.additional_scopes:
        configured_scopes.extend(args.additional_scopes)

    main(args.client_secrets_path, configured_scopes)
