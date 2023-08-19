CREATE TABLE IF NOT EXISTS {}(
tick INTEGER,
game_id INTEGER,
{cols},
FOREIGN KEY (tick) REFERENCES build_order(tick),
FOREIGN KEY (game_id) REFERENCES build_order(game_id),
PRIMARY KEY (tick, game_id));
