class SqlQueries:
    songplay_table_insert = ("""
        SELECT
            score_id, dim_country_code, dim_time_year, dim_indicator_code, value
        FROM score
    """)

    dim_country_insert = ("""
        SELECT distinct code, name, region, sub_region
        FROM country
    """)

    time_table_insert = ("""
        SELECT distinct year, decade, century
        FROM time_period
    """)

    indicator_table_insert = ("""
        SELECT distinct code, name, group
        FROM indicator
    """)
