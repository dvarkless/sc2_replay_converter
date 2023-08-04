CREATE TABLE IF NOT EXISTS game_info(
game_id SERIAL PRIMARY KEY,
timestamp_played TIMESTAMP NOT NULL,
date_processed TIMESTAMP,
players_hash VARCHAR(64) NOT NULL,
end_time INTEGER CHECK (end_time > 0),
player_1_id INTEGER NOT NULL,
player_1_race VARCHAR(1) NOT NULL,
player_1_league INTEGER,
player_2_id INTEGER NOT NULL,
player_2_race VARCHAR(1) NOT NULL,
player_2_league INTEGER,
map_hash VARCHAR(50) NOT NULL,
matchup VARCHAR(5) NOT NULL,
is_ladder BOOLEAN,
replay_path VARCHAR(250),
FOREIGN KEY (player_1_id) REFERENCES player_info(player_id),
FOREIGN KEY (player_2_id) REFERENCES player_info(player_id),
FOREIGN KEY (map_hash) REFERENCES map_info(map_hash));
