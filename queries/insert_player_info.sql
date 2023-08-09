INSERT INTO player_info(player_id, nickname, games_played, zerg_played, protoss_played, terran_played, wins, loses, most_played_race, highest_league)
VALUES (%(player_id)s, %(nickname)s, %(games_played)s, %(zerg_played)s, %(protoss_played)s, %(terran_played)s, %(wins)s, %(loses)s, %(most_played_race)s, %(highest_league)s);
