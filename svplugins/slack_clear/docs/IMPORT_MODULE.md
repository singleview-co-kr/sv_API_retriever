https://brown.ezphp.net/entry/Slack-%EC%B1%84%EB%84%90-%EA%B8%B0%EB%A1%9D-%EC%9D%BC%EA%B4%84-%EC%82%AD%EC%A0%9C
batch erase all slack msg in channel 
    . access to https://api.slack.com/apps 
    . Create New App -> designate App title & Slack Workspace
    . choose left navigation -> OAuth & Permissions
    . User Token Scopes -> add privileges like below
    users:read
    channels:read
    channels:history
    chat:write
    files:write
    . click Install App to Workspace to install App
    . permission agree (Allow)
    . copy OAuth Access Token and paste to ./conf/slack_config.ini -> slack_user_oauth_token
    . pip3.X install slack-cleaner2