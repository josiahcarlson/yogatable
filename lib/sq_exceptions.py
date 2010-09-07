
class YogaTableException(Exception):
    def __init__(self, string, *format):
        if format:
            Exception.__init__(self, string % format)
        else:
            Exception.__init__(self, string)

class IndexWarning(YogaTableException):
    pass

class TableIndexError(YogaTableException):
    pass

class ColumnException(YogaTableException):
    pass

class TableNameError(YogaTableException):
    pass


class PackError(YogaTableException):
    pass

class IndexRowTooLong(PackError):
    pass

class TooManyIndexRows(PackError):
    pass


class InvalidOperation(YogaTableException):
    pass

class UnknownExceptionError(YogaTableException):
    pass

BAD_NAMES = frozenset('''
    ABORT ADD AFTER ALL ALTER ANALYZE AND AS ASC ATTACH AUTOINCREMENT BEFORE
    BEGIN BETWEEN BY CASCADE CASE CAST CHECK COLLATE COLUMN COMMIT CONFLICT
    CONSTRAINT CREATE CROSS CURRENT_DATE CURRENT_TIME CURRENT_TIMESTAMP
    DATABASE DEFAULT DEFERRABLE DEFERRED DELETE DESC DETACH DISTINCT DROP EACH
    ELSE END ESCAPE EXCEPT EXCLUSIVE EXISTS EXPLAIN FAIL FOR FOREIGN FROM FULL
    GLOB GROUP HAVING IF IGNORE IMMEDIATE IN INDEX INDEXED INITIALLY INNER
    INSERT INSTEAD INTERSECT INTO IS ISNULL JOIN KEY LEFT LIKE LIMIT MATCH
    NATURAL NOT NOTNULL NULL OF OFFSET ON OR ORDER OUTER PLAN PRAGMA PRIMARY
    QUERY RAISE REFERENCES REGEXP REINDEX RELEASE RENAME REPLACE RESTRICT
    RIGHT ROLLBACK ROW SAVEPOINT SELECT SET TABLE TEMP TEMPORARY THEN TO
    TRANSACTION TRIGGER UNION UNIQUE UPDATE USING VACUUM VALUES VIEW VIRTUAL
    WHEN WHERE _ROWID_ MAIN OID ROWID SQLITE_MASTER SQLITE_SEQUENCE
    SQLITE_TEMP_MASTER TEMP'''.lower().split())
