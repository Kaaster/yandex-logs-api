import requests
from time import sleep as time_sleep
from json import dumps as json_dumps
from csv import reader as csv_reader, writer as csv_writer


class AppMetricaLogsAPI:

    DEFAULT_TIMEOUT = 30
    DEFAULT_SLEEP_SEC = 30

    def __init__(self, application_id, access_token, cache_option=None):
        self.application_id = application_id
        self.access_token = access_token
        self.headers = {
            'Accept': 'application/json',
            'Authorization': f'OAuth {access_token}'
        }

        if cache_option is not None:
            if cache_option == 0:
                self.headers['Cache-Control'] = 'no-cache'
            else:
                self.headers['Cache-Control'] = f'max-age={cache_option}'

    @property
    def request_credentials(self):
        return dict(counter_id=self.counter_id, access_token=self.access_token, headers=self.headers)

    def get_logrequest(self, params, source, url=None, output_type='json', timeout=DEFAULT_TIMEOUT):
        
        if url is None:
            url = f'https://api.appmetrica.yandex.ru/logs/v1/export/{source}.{output_type}?application_id={self.application_id}'
        
        response = requests.get(url=url, params=params, headers=self.headers, timeout=timeout)
        
        return response
    
    @staticmethod
    def response_content_to_json(response_json, filename):
        """
        Сохранение данных в json формате
        """
        response = json_dumps(response_json)

        with open(filename, mode='w', encoding='utf-8') as file:
            file.write(response)

        print(f'data saved to {filename}')

    @staticmethod
    def response_content_to_csv(response_csv, filename):
        """
        Сохранение данных в csv формате
        """
        response_csv = csv_reader(response_csv.decode('utf-8').splitlines(), delimiter='\t')
        response_csv = [row for row in response_csv]

        with open(filename, mode='w', encoding='utf-8', newline='') as file:
            writer = csv_writer(file)
            writer.writerows(response_csv)

        print(f'data saved to {filename}')
    
    def make_job(self, params, source, output_type='json', timeout=DEFAULT_TIMEOUT, sleep_sec=DEFAULT_SLEEP_SEC):

        for _ in range(100):
            result = self.get_logrequest(params=params, source=source, output_type=output_type, timeout=timeout)
            result_status_code = result.status_code
            print(result_status_code)

            if result_status_code == 202:
                time_sleep(sleep_sec)
                continue
            elif result_status_code == 200:
                date_since = params.get('date_since', '')
                date_until = params.get('date_until', '')
                filename = f'{source}_{date_since}_{date_until}.{output_type}'
                
                if output_type == 'json':
                    self.response_content_to_json(result.json(), filename)
                elif output_type == 'csv':
                    self.response_content_to_csv(result.content, filename)
                else:
                    print('result doesnt save to file')
                
            break
        
        return result.headers
