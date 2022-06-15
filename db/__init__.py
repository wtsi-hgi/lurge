import db.common
import db.warnings

import db_config

historical_usage = db.warnings.get_all_historical_usage_data(
    db.common.get_sql_connection(db_config))
