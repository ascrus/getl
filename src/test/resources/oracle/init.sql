CREATE TABLE test_func (
    id int NOT NULL PRIMARY KEY,
    name varchar(50) NOT NULL,
    d1 date NOT NULL,
    d2 timestamp NOT NULL,
    d3 timestamp with time zone NOT NULL,
    d4 timestamp with local time zone NOT NULL
);

DROP PACKAGE P_TEST_FUNC;

CREATE OR REPLACE PACKAGE DEVELOPER.P_TEST_FUNC IS
    TYPE rowTestFunc IS RECORD(
        id int,
        name varchar(50),
        d1 date,
        d2 timestamp,
        d3 timestamp with time zone,
        d4 timestamp with local time zone
    );
    TYPE tableTestFunc IS TABLE OF rowTestFunc;
    FUNCTION testFunc(start_id int, finish_id int)
    RETURN tableTestFunc
    PIPELINED;
END P_TEST_FUNC;

CREATE OR REPLACE PACKAGE BODY DEVELOPER.P_TEST_FUNC IS
    FUNCTION testFunc(start_id int, finish_id int)
    RETURN tableTestFunc
    PIPELINED
    IS
    BEGIN
        FOR cur IN (
            SELECT t.id, t.name, t.d1, t.d2, t.d3, t.d4
            FROM developer.test_func t
            WHERE (start_id IS NULL OR t.id >= start_id) AND (finish_id IS NULL OR t.id <= finish_id)
        ) LOOP
            PIPE row(cur);
        END LOOP;
    END testFunc;
END P_TEST_FUNC;

SELECT * FROM user_errors WHERE name = 'P_TEST_FUNC';

SELECT * FROM TABLE(DEVELOPER.P_TEST_FUNC.testFunc(1, 9));
