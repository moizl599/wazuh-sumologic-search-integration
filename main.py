from http.cookiejar import MozillaCookieJar
import requests
import json
import time
import os
import datetime
# Get the current time
current_time = datetime.datetime.now()

# Calculate the time 30 minutes ago
from_time = current_time - datetime.timedelta(minutes=30)

logfile='loginas_feature_log.log'
search_query = {
    "query": "(_sourceCategory=prod*/app/logs/api-core* or _sourceCategory=uat*/app/logs/api-core*) and \"The admin user\"\n| json \"message.data.resource.labels.cluster_name\" as cluster \n| json \"message.data.textPayload\" as payload  \n| json \"message.publish_time\" as publish_time  \n| parse field=payload \"The admin user * is logging in as *.\" as user, impersonated\n| count cluster, user, impersonated, publish_time",
    "from": from_time.strftime("%Y-%m-%dT%H:%M:%S"),
    "to": current_time.strftime("%Y-%m-%dT%H:%M:%S"),
    "timeZone": "EST",
    "byReceiptTime": True
}

data = json.dumps(search_query, indent=2)
#print(data)
cookies = MozillaCookieJar('cookies.txt')

headers = {
    'Content-type': 'application/json',
    'Accept': 'application/json',
}

with requests.Session() as session:
    session.cookies = cookies
    response = session.post(
        'https://api.us2.sumologic.com/api/v1/search/jobs',
        headers=headers,
        data=data,
        auth=('su2Oqh4J1spSoQ', 'ACCESS-KEY'),
    )
    cookies.save(ignore_discard=True, ignore_expires=True)
search_job_extracted = json.loads(response.text)
search_job_link = search_job_extracted['link']['href']

# Waiting for the search result to finish
search_result_state = 'GATHERING RESULTS'
while search_result_state == 'GATHERING RESULTS':
    headers = {
        'Accept': 'application/json',
    }

    with requests.Session() as session:
        session.cookies = cookies
        response = session.get(
            f'{search_job_link}',
            headers=headers,
            auth=('ACCESS-ID', 'ACCESS-KEY'),
        )
        cookies.save(ignore_discard=True, ignore_expires=True)
        search_result = json.loads(response.text)
        # print("Waiting 10 seconds")
        time.sleep(10)
        search_result_state = search_result['state']
        #print(search_result_state)

# print(search_result_state)

headers = {
    'Accept': 'application/json',
}
params = {
    'offset': '0',
    'limit': '10000',
}
# Extracting search results
with requests.Session() as session:
    session.cookies = cookies
    response = session.get(
        f'{search_job_link}/messages',
        params=params,
        headers=headers,
        auth=('ACCESS-ID', 'ACCESS-KEY'),
    )
    cookies.save(ignore_discard=True, ignore_expires=True)
    data = json.loads(response.text)
    logs=data['messages']
    if os.path.isfile(logfile):
        os.remove(logfile)
    with open(logfile, 'x') as logs_file:
        for log_line in logs:
            loginas_logs = {'log_type': 'applog-Login-as-feature-usage',
                                   'namespace': log_line['map']['namespace'],
                                   'user': log_line['map']['user'],
                                   'impersonated': log_line['map']['impersonated'],
                                   'payload': log_line['map']['payload'],
                                   }
            print(loginas_logs)
            with open(logfile, 'a') as f:
                json.dump(loginas_logs, f)
                f.write('\n')



    #with open('search_result.json', 'w') as outfile:
        #json.dump(data, outfile, ensure_ascii=False, indent=4)

# Deleting the search Job
with requests.Session() as session:
    session.cookies = cookies
    response = session.delete(
        search_job_link,
        headers=headers,
        auth=('ACCESS-ID', 'ACCESS-KEY'),
    )
    cookies.save(ignore_discard=True, ignore_expires=True)

#print(json.loads(response.text))
