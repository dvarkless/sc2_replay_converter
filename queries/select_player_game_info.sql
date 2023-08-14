SELECT (player_1_race player_1_winner, player_2_race, player_2_winner) FROM game_info
WHERE game_id = %(game_id)s;
