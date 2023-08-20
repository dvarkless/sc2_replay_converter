CREATE TABLE IF NOT EXISTS {}(
tick INTEGER,
game_id INTEGER,
{cols},
FOREIGN KEY (game_id) REFERENCES game_info(game_id),
PRIMARY KEY (tick, game_id));
