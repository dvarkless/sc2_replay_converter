CREATE TABLE IF NOT EXISTS {}(
tick INTEGER FOREIGN KEY REFERENCES build_order,
game_id INTEGER FOREIGN KEY REFERENCES build_order,
{cols},
PRIMARY KEY (tick, game_id),
);
