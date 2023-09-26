"""
Author: Moiz Lakdawala
"""
import json
import os
import time
import datetime
import requests
from http.cookiejar import MozillaCookieJar

def main(config_file='config.json'):
    # Load configurations from config.json
    with open(config_file) as f:
        config = json.load(f)
    
    # Set up the necessary variables
    current_time = datetime.datetime.now()
    from_time = current_time - datetime.timedelta(minutes=30)
    logfile = config.get('logfile')
    search_query = {
        "query": config.get('query'),
        "from": from_time.strftime("%Y-%m-%dT%H:%M:%S"),
        "to": current_time.strftime("%Y-%m-%dT%H:%M:%S"),
        "timeZone": config.get('timeZone'),
        "byReceiptTime": config.get('byReceiptTime')
    }
    
    # Initialize cookies and headers
    cookies = MozillaCookieJar(config.get('cookie_file'))
    headers = {'Content-type': 'application/json', 'Accept': 'application/json'}
    
    # Send the POST request
    with requests.Session() as session:
        session.cookies = cookies
        response = session.post(
            config.get('url'),
            headers=headers,
            data=json.dumps(search_query),
            auth=(config.get('access_id'), config.get('access_key')),
        )
        cookies.save(ignore_discard=True, ignore_expires=True)
    
    search_job_link = json.loads(response.text)['link']['href']
    
    # Loop until the search result is ready
    while True:
        with requests.Session() as session:
            session.cookies = cookies
            response = session.get(
                search_job_link,
                headers=headers,
                auth=(config.get('access_id'), config.get('access_key')),
            )
            cookies.save(ignore_discard=True, ignore_expires=True)
            search_result = json.loads(response.text)
            if search_result['state'] != 'GATHERING RESULTS':
                break # break the while loop once the results are gethered
            time.sleep(10)
    
    # Extracting search results
    with requests.Session() as session:
        session.cookies = cookies
        response = session.get(
            f'{search_job_link}/messages',
            params={'offset': '0', 'limit': '10000'},
            headers=headers,
            auth=(config.get('access_id'), config.get('access_key')),
        )
        cookies.save(ignore_discard=True, ignore_expires=True)
        logs = json.loads(response.text)['messages']
        if os.path.isfile(logfile):
            os.remove(logfile)
        with open(logfile, 'x') as logs_file:
            for log_line in logs:
                loginas_logs = {'log_type': 'Sumo_logic_payload', 'payload': log_line['map']['payload']}
                with open(logfile, 'a') as f:
                    json.dump(loginas_logs, f)
                    f.write('\n')
    
    # Deleting the search Job
    with requests.Session() as session:
        session.cookies = cookies
        response = session.delete(search_job_link, headers=headers, auth=(config.get('access_id'), config.get('access_key')))
        cookies.save(ignore_discard=True, ignore_expires=True)


if __name__ == "__main__":
    main()
