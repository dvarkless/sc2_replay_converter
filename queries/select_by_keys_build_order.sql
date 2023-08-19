SELECT * FROM build_order
WHERE
game_id = %(game_id)s
AND
tick = %(tick)s;
