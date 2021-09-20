import db.common
import db.report

import db_config as config

# Pull all the DB data for the Warnings calculations
# Its a lot of data, so we want to ALL of it in ONE go
_conn = db.common.getSQLConnection(config)
historical_usage = db.report.get_all_historical_usage_data(_conn)
