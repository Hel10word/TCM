
-- https://dev.mysql.com/doc/refman/5.7/en/json.html#json-values

CREATE TABLE  json_types_mysql(
    myjson JSON
);


INSERT INTO `json_types_mysql` (`myjson`) VALUES ('[\"abc\", 10, null, true, false]');
INSERT INTO `json_types_mysql` (`myjson`) VALUES ('{\"k1\": \"value\", \"k2\": 10}');
INSERT INTO `json_types_mysql` (`myjson`) VALUES ('[99, {\"id\": \"HK500\", \"cost\": 75.99}, [\"hot\", \"cold\"]]');
INSERT INTO `json_types_mysql` (`myjson`) VALUES ('{\"k1\": \"value\", \"k2\": [10, 20]}');
