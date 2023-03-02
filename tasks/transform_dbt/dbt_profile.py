import os
from pathlib import Path
import yaml

from dotenv import load_dotenv

"""
Generate profiles.yml file for dbt project based on .env vars
"""

if __name__ == "__main__":
    load_dotenv(dotenv_path=str(Path(__file__).parent.parent.parent.resolve()) + '/.env')
    profiles = dict(
        ci_data = dict(
            target = 'warehouse',
            outputs = dict(
                warehouse = dict(
                    type = 'postgres',
                    host = os.environ['RDS_HOST'],
                    user = os.environ['RDS_USER'],
                    password = os.environ['RDS_PW'],
                    port = int(os.environ['RDS_PORT']),
                    dbname = 'postgres',
                    schema = 'warehouse',
                    threads = 4,
                    keepalives_idle = 0
                )
            )
        )
    )

    with open(str(Path.home()) + "/.dbt/profiles.yml", "w") as f:
        yaml.dump(profiles, f, default_flow_style=False)