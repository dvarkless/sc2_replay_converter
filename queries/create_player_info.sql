CREATE TABLE IF NOT EXISTS player_info(
player_id INTEGER PRIMARY KEY,
nickname VARCHAR(12),
games_played INTEGER,
zerg_played INTEGER,
protoss_played INTEGER,
terran_played INTEGER,
wins INTEGER,
loses INTEGER,
most_played_race VARCHAR(1),
highest_league INTEGER);

