import logging
import logging.config

from sqlalchemy import create_engine
from ETL.common.mysql import MysqlConnector
import yaml
from ETL.transform.ETL import FinanceETL, WebConfig, DBSourceConfig

# SET your own directory
config_path = "D:\DataProject\WebScraping\config\Income_report1_config.yml"

# Load configure file

config = yaml.safe_load(open(config_path, encoding='utf-8'))

# configure logging 
log_config = config['logging']
logging.config.dictConfig(log_config)
logger = logging.getLogger(__name__)

# reading source configuration
web_config = WebConfig(**config['web'])
# reading target configuration
mysql_config = DBSourceConfig(**config['mysql'])

# creating the S3BucketConnector class instance to connect to mysql
mysql_trg = MysqlConnector(host = mysql_config.host,
                                      user=mysql_config.user,
                                      password=mysql_config.password,
                                      database=mysql_config.database)

Income_ETL = FinanceETL(mysql_trg, web_config, mysql_config)
Income_ETL.income_ETL()
