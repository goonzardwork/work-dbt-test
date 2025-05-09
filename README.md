## Caveats

### 0. Local Start

```bash
uv venv
source .venv/bin/activate

uv pip install -r requirements.txt
```

### 1. Profile Setup

```yaml
# ~/.dbt/profiles.yml 
fund_dw:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      user: postgres
      password: postgres
      port: 5432
      dbname: dw
      schema: public
      threads: 1

fund_dbt:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      user: postgres
      password: postgres
      port: 5432
      dbname: postgres
      schema: bi
      threads: 1
```

### 2. Cross Database Setup

```sql
-- On postgres (fund_dbt connection target)
CREATE EXTENSION IF NOT EXISTS postgres_fdw;

CREATE SERVER dw_server
  FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (host 'localhost', dbname 'dw', port '5432');

CREATE USER MAPPING FOR postgres
  SERVER dw_server
  OPTIONS (user 'postgres', password 'postgres');

CREATE SCHEMA IF NOT EXISTS igisam;
IMPORT FOREIGN SCHEMA igisam FROM SERVER dw_server INTO igisam;

CREATE SCHEMA IF NOT EXISTS igisbiz;
IMPORT FOREIGN SCHEMA igisbiz FROM SERVER dw_server INTO igisbiz;

CREATE SCHEMA IF NOT EXISTS igisdms;
IMPORT FOREIGN SCHEMA igisdms FROM SERVER dw_server INTO igisdms;

```

DBT does not support querying across databases. 
Each DBT connection can only talk to **one Postgres database** at a time.


### 3. Run

```bash
dbt debug  # Check connection
dbt run    # Build models
dbt test
dbt docs serve
```