import requests
from datetime import datetime
from time import sleep as time_sleep
from csv import reader as csv_reader, writer as csv_writer


class YandexMetricaLogsAPI:
    """
    Класс для работы с Logs API Яндекс.Метрики
    """

    DEFAULT_VALUE = 'unknown'
    DATE_FORMAT = '%Y-%m-%d'
    DEFAULT_TIMEOUT = 30
    DEFAULT_SLEEP_SEC = 30
    BYTES_PER_GB = 1e9

    def __init__(self, counter_id, access_token):
        """
        :param counter_id: номер счетчика
        :param access_token: токен
        """
        self.access_token = access_token
        self.counter_id = counter_id
        self.headers = {
            'Accept': 'application/json',
            'Content-Encoding': 'gzip',
            'Authorization': f'OAuth {access_token}',
        }

    @property
    def request_credentials(self):
        return dict(counter_id=self.counter_id, access_token=self.access_token, headers=self.headers)

    def handle_bad_response(self, response):
        return dict(status_code=response.status_code, message=response.json().get('message', self.DEFAULT_VALUE))

    @staticmethod
    def response_content_to_csv(request_id, part_number, content):
        """
        Сохранение данных в csv формате
        """
        content = csv_reader(content.decode('utf-8').splitlines(), delimiter='\t')
        content = [row for row in content]
        file_name = f'{request_id}-{part_number}.csv'

        with open(file_name, mode='w', encoding='utf-8', newline='') as file:
            writer = csv_writer(file)
            writer.writerows(content)

        print(f'data saved to {file_name}')

    def query_metrics(self, params, response_json):
        response_json = response_json.get('log_request_evaluation', {})
        return dict(
            possible=response_json.get('possible'),
            query_params=dict(
                size_requested_gb=response_json.get('expected_size', 0.0) / self.BYTES_PER_GB,
                days_requested=(datetime.strptime(params.get('date2'), self.DATE_FORMAT) -
                                datetime.strptime(params.get('date1'), self.DATE_FORMAT)).days + 1,
            ),
            max_possible_params=dict(
                max_possible_size_gb=response_json.get('log_request_sum_max_size', 0.0) / self.BYTES_PER_GB,
                max_possible_days=response_json.get('max_possible_day_quantity', 0)
            )
        )

    def get_logrequests(self, timeout=DEFAULT_TIMEOUT):
        """
        Запрос списока запрошенных логов
        """
        url = f'https://api-metrika.yandex.net/management/v1/counter/{self.counter_id}/logrequests'
        response = requests.get(url=url, headers=self.headers, timeout=timeout)

        if response.status_code == 200:
            return response.json()

        return self.handle_bad_response(response)

    def get_logrequests_evaluate(self, params, timeout=DEFAULT_TIMEOUT):
        """
        Проверка возможности выполненяи запроса
        """
        url = f'https://api-metrika.yandex.net/management/v1/counter/{self.counter_id}/logrequests/evaluate'
        response = requests.get(url=url, headers=self.headers, params=params, timeout=timeout)

        if response.status_code == 200:
            return self.query_metrics(params, response.json())

        return self.handle_bad_response(response)

    def post_logrequests(self, params, timeout=DEFAULT_TIMEOUT):
        """
        Запрос на подготовку логов
        """
        url = f'https://api-metrika.yandex.net/management/v1/counter/{self.counter_id}/logrequests'
        response = requests.post(url=url, headers=self.headers, params=params, timeout=timeout)

        if response.status_code == 200:
            return dict(request_id=response.json().get('log_request', {}).get('request_id', self.DEFAULT_VALUE))

        return self.handle_bad_response(response)

    def get_logrequests_result(self, request_id, timeout=DEFAULT_TIMEOUT):
        """
        Статус подготовки логов
        """
        url = f'https://api-metrika.yandex.net/management/v1/counter/{self.counter_id}/logrequest/{request_id}'
        response = requests.get(url=url, headers=self.headers, timeout=timeout)

        if response.status_code == 200:
            status = response.json().get('log_request', {}).get('status', self.DEFAULT_VALUE)

            if status == 'processed':
                parts = [part.get('part_number', -1) for part in response.json().get('log_request', {}).get('parts', [])]
                return dict(request_id=request_id, status=status, parts=parts)

            return dict(request_id=request_id, status=status)

        return self.handle_bad_response(response)

    def get_logrequest_download(self, request_id, part_number, timeout=DEFAULT_TIMEOUT):
        """
        Скачивание готовых данных
        """
        url = f'https://api-metrika.yandex.net/management/v1/counter/{self.counter_id}/logrequest/{request_id}/part/{part_number}/download'
        response = requests.get(url=url, headers=self.headers, timeout=timeout)

        if response.status_code == 200:
            return dict(content=response.content)

        return self.handle_bad_response(response)

    def post_logrequest_clean(self, request_id, timeout=DEFAULT_TIMEOUT):
        """
        Очистка запрошенных логов по request_id
        """
        url = f'https://api-metrika.yandex.net/management/v1/counter/{self.counter_id}/logrequest/{request_id}/clean'
        response = requests.post(url=url, headers=self.headers, timeout=timeout)

        if response.status_code == 200:
            return dict(status_code=response.status_code, message=f'request {request_id} deleted')

        return self.handle_bad_response(response)

    def post_logrequest_cancel(self, request_id, timeout=DEFAULT_TIMEOUT):
        """
        Отмена еще не обработанного запроса логов по request_id
        """
        url = f'https://api-metrika.yandex.net/management/v1/counter/{self.counter_id}/logrequest/{request_id}/cancel'
        response = requests.post(url=url, headers=self.headers, timeout=timeout)

        if response.status_code == 200:
            return dict(status_code=response.status_code, message=f'request {request_id} canceled')

        return self.handle_bad_response(response)

    def make_job(self, params, timeout=DEFAULT_TIMEOUT, sleep_sec=DEFAULT_SLEEP_SEC):
        gle = self.get_logrequests_evaluate(params=params, timeout=timeout)

        if gle.get('possible', False):
            pl = self.post_logrequests(params=params, timeout=timeout)
            request_id = pl.get('request_id', None)
            query_params = gle.get('query_params', self.DEFAULT_VALUE)

            if request_id is not None:
                print(f'request_id: {request_id}. query_params: {query_params}')

                for _ in range(100):
                    time_sleep(sleep_sec)
                    glr = self.get_logrequests_result(request_id=request_id, timeout=timeout)
                    status = glr.get('status', None)

                    if status is None or status not in ['created', 'processed']:
                        return glr
                    elif status == 'created':
                        print(f'#{_} {glr}')
                    else:
                        for part in glr.get('parts', []):
                            gld = self.get_logrequest_download(request_id=request_id, part_number=part, timeout=timeout)

                            if gld.get('content', None) is not None:
                                self.response_content_to_csv(request_id=request_id, part_number=part, content=gld.get('content', None))
                            else:
                                print(gld)
                        return self.post_logrequest_clean(request_id=request_id, timeout=timeout)
            else:
                return pl
        else:
            return gle
