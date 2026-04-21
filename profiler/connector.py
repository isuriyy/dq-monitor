from sqlalchemy import create_engine, inspect
import yaml

class DBConnector:
    def __init__(self, config_path="config/sources.yaml"):
        with open(config_path) as f:
            self.config = yaml.safe_load(f)

    def get_engine(self, source_name):
        src = next(s for s in self.config["sources"] if s["name"] == source_name)
        if src["dialect"] == "sqlite":
            url = f"sqlite:///{src['path']}"
        elif src["dialect"] == "postgresql":
            url = f"postgresql://{src['user']}:{src['password']}@{src['host']}:{src['port']}/{src['database']}"
        else:
            raise ValueError(f"Unsupported dialect: {src['dialect']}")
        return create_engine(url)

    def get_tables(self, engine):
        return inspect(engine).get_table_names()

    def get_columns(self, engine, table):
        return inspect(engine).get_columns(table)
