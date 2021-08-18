<div style="margin: auto">
<h1>Zendesk to Big Query Pipeline</h1>
</div>


- 💻 code to extract all tickets and metrics about tickets from Zendesk API
- ✔️ This code respect the API rate limit 
- 📈 Loads the data into a big query table for analysis by the business area

##

<div style="margin: auto">
<h2>How to use this code</h2>
</div>

1.install the packages:
- google-cloud-bigquery
- logging
- os
- shutil
- datetime import datetime as dt
- re
- requests as rq
- json
- pandas as pd
- urllib.parse
- numpy

You can install packeges with one of this commands:
- prompt command
```
pip install package-name-here
```
- Or you can install with python code
```
os.system('pip install package-name-here')
```

2.Change the authentication parameters directory
change directory to the json file of your google cloud platform service account
```
SERVICE_ACCOUNT_JSON = r'C:\pipelines\auth\paylivre-304215-78a2e9fec1be.json'
```

change to the name of the table you will load the data into BigQuery ("projectname.dataset.tablename")
```
table_id = "paylivre-304215.suporte.zendesk_tickets"
```

change url to your subdomain
```
url = 'https://yoursubdomain.zendesk.com/api/v2/search.json?' + urlencode(params)
```

Be happy with your zendesk extractor ☺️
