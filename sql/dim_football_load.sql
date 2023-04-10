INSERT INTO DIM_FOOTBALL_TABLE_NAME
select
    FIELDS_VALUE
from OBJ_FOOTBALL_TABLE_NAME
, lateral flatten(input => raw) as r;