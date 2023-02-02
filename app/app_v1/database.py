import pymysql
import snowflake.connector


HOST = 'electionkanoml.cypjjjls3xgo.eu-west-1.rds.amazonaws.com'
# HOST ='election.cypjjjls3xgo.eu-west-1.rds.amazonaws.com'
USER = 'admin'
PASS = 'election1234'
db='test'



def get_db():
    return pymysql.connect(
        host=HOST,
        user=USER,
        password=PASS,
        db=db,
        port=3306,
        cursorclass=pymysql.cursors.DictCursor
    )

def get_db2():
    return snowflake.connector.connect(

    user= 'ismail',
    password = 'Ismail01?',
    account = 'wagtiji-ky72580',
    database='DATA',
    schema = 'PUBLIC',
    warehouse='TEST'
)

# def get_db2():
#     return snowflake.connector.connect(

#     user= 'sjdhsn',
#     password = 'Sjdhsn.567',
#     account = 'vozrhej-ym73300',
#     database='DATA',
#     schema = 'PUBLIC',
#     warehouse='TEST'
# )

# def get_db2():
#     return snowflake.connector.connect(

#     user= 'jameel',
#     password = 'Jamilu01?',
#     account = 'pgvbhpz-jq78554',
#     database='MYDATA',
#     schema = 'PUBLIC',
#     warehouse='MYTEST'
# )


# def get_db2():
#     return snowflake.connector.connect(

#     user= 'maniz',
#     password = 'Election@1234',
#     account = 'hutnsjf-xg56064',
#     database='DATA',
#     schema = 'PUBLIC',
#     warehouse='TEST',
#     ABORT_DETACHED_QUERY = True
# )

# def get_db():
#     return 1
