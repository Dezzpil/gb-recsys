import os
import time
import requests
import pandas as pd
from datetime import datetime, date, timedelta
from sqlalchemy import create_engine, Table, Column, Integer, Float, DateTime, Date, BigInteger, MetaData
from config import config

class MetrikaExporter:
    def __init__(self):
        self.counter_id = config.METRIKA_COUNTER_ID
        self.headers = {
            'Authorization': f'OAuth {config.METRIKA_OAUTH_TOKEN}',
            'Content-Type': 'application/x-yametrika+json'
        }
        self.columns = config.METRIKA_COLUMNS
        self.fields = ','.join(self.columns)
        self.data_dir = config.METRIKA_DATA_DIR
        os.makedirs(self.data_dir, exist_ok=True)
        
        self.engine = create_engine(config.DB_URL)
        self.metadata = MetaData()
        self.logs_table = Table(
            'metrika_logs', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('start_time', DateTime),
            Column('duration', Float),
            Column('period_start', Date),
            Column('period_end', Date),
            Column('file_size', BigInteger),
            Column('records_count', Integer)
        )

    def evaluate_and_request(self, date1, date2):
        url = f'https://api-metrika.yandex.net/management/v1/counter/{self.counter_id}/logrequests/evaluate'
        params = {
            'date1': date1,
            'date2': date2,
            'fields': self.fields,
            'source': 'visits'
        }
        r = requests.get(url, params=params, headers=self.headers)
        r.raise_for_status()
        response = r.json()
        
        request_id = None
        if response['log_request_evaluation']['possible']:
            url = f'https://api-metrika.yandex.net/management/v1/counter/{self.counter_id}/logrequests'
            r = requests.post(url, params=params, headers=self.headers)
            r.raise_for_status()
            response = r.json()
            request_id = response['log_request']['request_id']
            print(f'Request ID created: {request_id} for period {date1} - {date2}')
        else:
            print(f'Evaluation failed for period {date1} - {date2}: {response}')
            
        return request_id

    def wait_for_completion(self, request_id):
        url = f'https://api-metrika.yandex.net/management/v1/counter/{self.counter_id}/logrequests'
        while True:
            r = requests.get(url, headers=self.headers)
            r.raise_for_status()
            status_data = r.json()
            
            for req in status_data['requests']:
                if req['request_id'] == request_id:
                    status = req['status']
                    if status == 'processed':
                        return req
                    elif status == 'cleaned' or status == 'cleaning_in_progress':
                        raise Exception(f"Request {request_id} was cleaned before download.")
                    elif status == 'error':
                        raise Exception(f"Request {request_id} failed with error.")
                    
                    print(f"Request {request_id} status: {status}. Waiting 30 seconds...")
                    time.sleep(30)
                    break
            else:
                raise Exception(f"Request {request_id} not found.")

    def download_and_clean(self, request_id):
        request_info = self.wait_for_completion(request_id)
        parts_num = len(request_info['parts'])
        downloaded_files = []
        
        for i in range(parts_num):
            url = f'https://api-metrika.yandex.net/management/v1/counter/{self.counter_id}/logrequest/{request_id}/part/{i}/download'
            r = requests.get(url, headers=self.headers)
            r.raise_for_status()
            
            file_path = os.path.join(self.data_dir, f'{request_id}-part{i}.csv')
            with open(file_path, 'wb') as f:
                f.write(r.content)
            
            print(f'Part {i} downloaded to {file_path}')
            downloaded_files.append(file_path)
            
        # Clean up on server
        url = f'https://api-metrika.yandex.net/management/v1/counter/{self.counter_id}/logrequest/{request_id}/clean'
        requests.post(url, headers=self.headers)
        print(f'Request {request_id} cleaned on server.')
        
        return downloaded_files

    def process_files(self, files, output_file):
        dfs = []
        for f in files:
            df_part = pd.read_csv(f, delimiter='\t')
            dfs.append(df_part)
            
        if not dfs:
            return pd.DataFrame(columns=self.columns)
            
        df = pd.concat(dfs)
        
        # Filter rows with empty ym:s:impressionsDateTime (as per notebook)
        # In notebook it was: df = df[(df['ym:s:impressionsDateTime'] != '[]')]
        if 'ym:s:impressionsDateTime' in df.columns:
            initial_count = len(df)
            df = df[df['ym:s:impressionsDateTime'] != '[]']
            print(f"Filtered out {initial_count - len(df)} rows with empty impressionsDateTime.")
        
        df.to_csv(output_file, index=False)
        
        # Remove partial files
        for f in files:
            os.remove(f)
            
        return df

    def log_to_db(self, start_time, duration, period_start, period_end, file_size, records_count):
        with self.engine.begin() as conn:
            conn.execute(self.logs_table.insert().values(
                start_time=start_time,
                duration=duration,
                period_start=period_start,
                period_end=period_end,
                file_size=file_size,
                records_count=records_count
            ))

    def run_export(self, start_date_str=None, end_date_str=None):
        overall_start_time = datetime.now()
        
        if end_date_str:
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
        else:
            end_date = date.today() - timedelta(days=1)
            
        if start_date_str:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
        else:
            start_date = end_date - timedelta(days=3*365) # 3 years
            
        print(f"Starting export for period: {start_date} to {end_date}")
        
        # Split period into yearly chunks (API limitation)
        chunks = []
        current_start = start_date
        while current_start <= end_date:
            current_end = min(current_start + timedelta(days=364), end_date)
            chunks.append((current_start.strftime('%Y-%m-%d'), current_end.strftime('%Y-%m-%d')))
            current_start = current_end + timedelta(days=1)
            
        all_downloaded_files = []
        for s_date, e_date in chunks:
            request_id = self.evaluate_and_request(s_date, e_date)
            if request_id:
                files = self.download_and_clean(request_id)
                all_downloaded_files.extend(files)
        
        if not all_downloaded_files:
            print("No data downloaded.")
            return

        output_filename = f"metrika_{start_date}_{end_date}.csv"
        output_path = os.path.join(self.data_dir, output_filename)
        
        df_final = self.process_files(all_downloaded_files, output_path)
        
        overall_end_time = datetime.now()
        duration = (overall_end_time - overall_start_time).total_seconds()
        file_size = os.path.getsize(output_path)
        records_count = len(df_final)
        
        self.log_to_db(
            overall_start_time,
            duration,
            start_date,
            end_date,
            file_size,
            records_count
        )
        
        print(f"Export finished. Saved to {output_path}. Logged to DB.")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Export data from Yandex.Metrika')
    parser.add_argument('--start', type=str, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', type=str, help='End date (YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    exporter = MetrikaExporter()
    exporter.run_export(args.start, args.end)
