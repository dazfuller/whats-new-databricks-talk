import csv
import random
from configparser import ConfigParser
from datetime import datetime, timedelta, timezone
from io import StringIO
from typing import List

from azure.storage.filedatalake import DataLakeFileClient, FileSystemClient

num_hours: int = 100

config: ConfigParser = ConfigParser()
config.read("config.ini")

start_date: datetime = datetime(2019, 10, 1, 0, 0, 0, 0, tzinfo=timezone.utc)
assets: List[int] = [x for x in range (15820, 15960)]

connection_sting: str = config.get("default", "connection_string")
remote_path: str = "ingest-data"

client: FileSystemClient
with FileSystemClient.from_connection_string(connection_sting, "landing") as client:
    for hour_offset in range(num_hours):
        file_date: datetime = start_date + timedelta(hours=hour_offset)
        output_filename: str = f"events-{file_date.strftime('%Y%m%d_%H%M%S')}.csv"

        content: StringIO
        with StringIO() as content:
            csv_writer: csv.writer = csv.writer(content)
            csv_writer.writerow(["asset", "ts", "value", "comments"])
            data: List = []
            for i in range(3600):
                asset: int = random.choice(assets)
                value: float = random.random()
                event_date: datetime = file_date + timedelta(seconds=i)
                comment: str = f"This is a random comment for asset: {asset}" if i % 13 == 0 else ""
                data.append([asset, int(event_date.timestamp()), value, comment])
            csv_writer.writerows(data)
            
            content.flush()
            
            remote_file_path = f"{remote_path}/{file_date.strftime('year=%Y/month=%m/day=%d')}/{output_filename}"

            file_client: DataLakeFileClient
            with client.create_file(remote_file_path) as file_client:
                file_data: bytes = content.getvalue().encode("utf-8")
                file_client.append_data(file_data, offset=0, length=len(file_data))
                file_client.flush_data(len(file_data))
