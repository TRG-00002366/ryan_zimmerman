What is the purpose of profiles.yml vs dbt_project.yml?
Profiles.yml is in your home directory and specifys connection profiles. (Which snowflake account and warehouse to use, username, password, etc.)
dbt_project.yml specifies which connection profile to use and paths for the given project.

Why do we use {{ env_var('SNOWFLAKE_PASSWORD') }} instead of the actual password?
Security, storing passwords permanently in plain text is bad practice. ENV values only exist for the session.

What directory contains your transformation SQL files?
snowflake_analytics/models