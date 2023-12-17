import snowflake.connector
import sf_key
con = snowflake.connector.connect(**sf_key.snowflake_conn)
cursor = con.cursor()
print("---append to master snowflake tabel---")
select_query = "COPY INTO POI_INDONESIA.PUBLIC.POI_INDONESIA_MASTER FROM @STG_SI_DEMO_STAGE_1 PATTERN = '.*csv' FILE_FORMAT =csv1 FORCE = True;"

cursor.execute(select_query)
con.commit()
con.close()