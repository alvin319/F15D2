-- PL/SQL Code for the APEX Mobile Application

-- Page 2 NBA Query
SELECT jt.name FROM apex_collections t,
JSON_TABLE(t.clob001, '$.name[*]' COLUMNS rid for ordinality, name varchar2(255) PATH '$') jt
WHERE t.collection_name = 'P2_DOREST_RESULTS'

-- Page 3 NBA Team
SELECT jt.team_name AS Team, jt2.conference_name AS Conference FROM apex_collections t,
JSON_TABLE(t.clob001, '$.team_name[*]' COLUMNS rid for ordinality, team_name varchar2(255) PATH '$') jt,
JSON_TABLE(t.clob001, '$.conference_name[*]' COLUMNS rid for ordinality, conference_name varchar2(255) PATH '$') jt2
WHERE t.collection_name = 'P3_DOREST_RESULTS' AND jt.rid = jt2.rid

-- Page 4 NBA Player
SELECT jt.player_name AS "Player Name", jt2.team_name AS "Team Name", jt3.player_position AS "Player Position", jt4.player_height AS "Player Height (cm)"
FROM apex_collections t,
JSON_TABLE(t.clob001, '$.player_name[*]' COLUMNS rid for ordinality, player_name varchar2(255) PATH '$') jt,
JSON_TABLE(t.clob001, '$.team_name[*]' COLUMNS rid for ordinality, team_name varchar2(255) PATH '$') jt2,
JSON_TABLE(t.clob001, '$.player_position[*]' COLUMNS rid for ordinality, player_position varchar2(255) PATH '$') jt3,
JSON_TABLE(t.clob001, '$.player_height[*]' COLUMNS rid for ordinality, player_height number PATH '$') jt4
WHERE t.collection_name = 'P4_DOREST_RESULTS' AND jt.rid = jt2.rid AND jt2.rid = jt3.rid AND jt3.rid = jt4.rid
