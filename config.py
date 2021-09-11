from dynaconf import Dynaconf, Validator

settings = Dynaconf(
    settings_files=["settings.toml", ".secrets.toml"],
    validators=[
        Validator("COLLECTOR.RDBMS", "COLLECTOR.CYCLES", "COLLECTOR.SLEEP", must_exist=True),
        Validator("COLLECTOR.RDBMS", is_in=["postgres", "mysql"]),
        Validator("REPOSITORY.TYPE", must_exist=True),
        Validator("REPOSITORY.TYPE", is_in=["in_memory", "sqlite"]),
    ]
)
