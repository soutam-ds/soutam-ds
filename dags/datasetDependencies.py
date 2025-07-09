# Dataset is avaliable for airflow 2.4+ version
from airflow.sdk import asset


@asset(
schedule="@daily",
uri="https://randomuser.me/api/",
)
def user(self)-> dict:
    import requests
    r = requests.get(self.uri)
    if r.status_code == 200:
        return r.json()
    else:
        raise Exception(f"Failed to fetch data: {r.status_code} - {r.text}")