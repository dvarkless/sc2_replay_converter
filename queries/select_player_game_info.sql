SELECT (end_time, player_1_race, player_1_winner, player_1_league, player_2_race, player_2_winner, player_2_league) FROM game_info
WHERE game_id = %(game_id)s;
