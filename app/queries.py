EXISTING_QUERY = """
SELECT 
	group_suggest.group_id 
	, is_online 
	, category3_id
	, same_district 
	, same_zone
	, same_post
	, n_neighbors
	-- , meet
	-- , meet10
	, total_rank
	, CASE WHEN n_visits > 100 THEN (same_district + same_zone + same_post + (n_neighbors > 0)) * 1.0 / 4 * (1 - 0.9 * online_share)
		   WHEN n_visits > 10 THEN (same_district + same_zone + same_post + (n_neighbors > 0)) * 1.0  / 4 * (1 - 0.5 * online_share)
		   ELSE (same_district + same_zone + same_post + (n_neighbors > 0)) * 1.0  / 4
	  END as offline_rank
	, CASE WHEN n_visits > 100 THEN is_online * 1.0  * online_share
		   WHEN n_visits > 10 THEN is_online * 0.5 * online_share
		   ELSE is_online * 0.1 * online_share
	  END as online_rank
	, ends_soon
FROM (
	SELECT 
		groups.group_id
		, max(is_online) as is_online
		, min(groups.category3_id) as category3_id
		, max(ends_soon) as ends_soon
		, coalesce(g_district, 0) = u_district AND u_district > 0 as same_district
		, coalesce(g_zone, 0) = u_zone AND u_zone > 0 as same_zone
		, coalesce(g_postal_code, 0) = u_postal AND u_postal > 0 as same_post
		, coalesce(n_neighbors, 0) as n_neighbors
		-- , coalesce(days_from_last_event, -1) as days_from_last_event
		, coalesce(friends.meet, 0) as meet
		, coalesce(friends.meet10, 0) as meet10
	FROM groups 
	INNER JOIN (SELECT group_id FROM group_timetable) as group_timetable ON groups.group_id = group_timetable.group_id
	LEFT JOIN group_locations ON groups.group_id = group_locations.group_id
	LEFT JOIN (
		SELECT u_postal, coalesce(zone_id, 0) as u_zone, coalesce(district_id, 0) as u_district
		FROM users 
		LEFT JOIN postal_map ON users.u_postal = postal_map.code
		WHERE user_id = ?
	) users ON 1=1
	LEFT JOIN model_neighbors ON groups.group_id = model_neighbors.group_id AND users.u_postal = model_neighbors.postal_code 
	LEFT JOIN (SELECT group_id, meet, meet10 FROM friends WHERE user_id = ?) as friends ON groups.group_id = friends.group_id
	WHERE is_available = 1 --AND (days_from_last_event IS NULL OR days_from_last_event < n_days_max)
	GROUP BY groups.group_id
) as group_suggest
INNER JOIN (
	SELECT 
		category_out
		, category_rank + rank_age + 2 * same as total_rank
	FROM (
		SELECT 
			base.category3_id as category_out
			-- , round(-0.94628879 - 0.07349178714361423 * coalesce(category_rank, 50) - 0.09995501644536509 * coalesce(rank_age, 50) + 1.0775831177458692 * coalesce(same, 0), 4) as total_rank
			, CAST(coalesce(category_rank, 200) as INT) as category_rank
			, CAST(coalesce(rank_age, 200) as INT) as rank_age
			, coalesce(same, 0) as same
			-- , round(-1 * coalesce(category_rank, 50) - 0.1 * coalesce(rank_age, 50) + 100 * coalesce(same, 0), 4) as total_rank
			, coalesce(will_continue, 0) as already_ok
			-- , coalesce(enough, 0) as enough
			, coalesce(days_from_last_event, -1) as days_from_last_event
			, coalesce(n_days_max, 1000) as n_days_max
		FROM (
			SELECT category3_id
			FROM categories
		) as base 
		LEFT JOIN (
			SELECT 
				category_out
				, sum(rank_cat) * 1.0 / sum(n_visits) as category_rank
				, sum(n_visits) as sum_visits
				, sum(category_in = category_out) > 0 as same
			FROM (
				SELECT category3_id, user_quantile as n_visits
				FROM user_category_quantile
				WHERE user_id = ?
			) as attend_history
			LEFT JOIN (
				SELECT category_in, category_out, rank_cat
				FROM model_categories
				WHERE rank_cat <= 6
				UNION ALL 
				SELECT category3_id, category3_id, 0
				FROM categories
			) as model_categories ON attend_history.category3_id = model_categories.category_in
			GROUP BY category_out
			ORDER BY category_rank ASC
		) rank_category ON base.category3_id = rank_category.category_out
		LEFT JOIN (
			SELECT category_out, rank_age
			FROM model_age
			INNER JOIN (
				SELECT age_group, is_woman
				FROM users
				WHERE user_id = ?
			) users ON model_age.is_woman = users.is_woman AND model_age.age_group = users.age_group
			WHERE rank_age <= 6
			ORDER BY rank_age ASC
		) rank_age ON base.category3_id = rank_age.category_out
		INNER JOIN (
			SELECT DISTINCT category3_id, 1 as is_available
			FROM groups 
			INNER JOIN (SELECT group_id FROM group_timetable) as group_timetable ON groups.group_id = group_timetable.group_id
			-- WHERE is_available = 1
		) group_availability ON base.category3_id = group_availability.category3_id
		LEFT JOIN (
			SELECT category3_id, max(finish_date), julianday('2023-03-01') - julianday(max(finish_date)) as days_from_last_event
			FROM attend_history
			WHERE user_id = ?
			GROUP BY category3_id
		) cat_days_left ON base.category3_id = cat_days_left.category3_id
		LEFT JOIN no_activity_limit ON cat_days_left.category3_id = no_activity_limit.category3_id
		LEFT JOIN (
			SELECT DISTINCT category3_id, 1 as will_continue
			FROM attend_history
			INNER JOIN (
				SELECT group_id
				FROM groups 
				WHERE is_available = 1 AND ends_soon = 0
			) groups ON attend_history.group_id = groups.group_id
			WHERE user_id = ? AND finish_date > '2023-02-01'
		) current_groups ON base.category3_id = current_groups.category3_id
		-- LEFT JOIN (
		-- 	SELECT category3_id, 1 as enough
		-- 	FROM attend_history		
		-- 	WHERE user_id = ?
		-- 	GROUP BY category3_id
		-- 	HAVING max(finish_date) < '2023-01-01' AND sum(n_visits) > 25
		-- ) enough_groups ON base.category3_id = enough_groups.category3_id
	)
	WHERE days_from_last_event < n_days_max AND already_ok = 0 
	ORDER BY (category_rank + rank_age) ASC 
) as category_suggest ON group_suggest.category3_id = category_suggest.category_out
LEFT JOIN (
	SELECT 
		sum(n_visits * is_online) * 1.0 / sum(n_visits) as online_share
		, sum(n_visits) as n_visits
	FROM attend_history
	JOIN groups ON attend_history.group_id = groups.group_id
	WHERE attend_history.user_id = ?
) as user_pref
ORDER BY total_rank ASC, (offline_rank + online_rank) DESC, offline_rank DESC, same_district DESC, meet10 = 0, meet = 0, (same_zone + (n_neighbors > 5)) DESC, same_post DESC, meet10 DESC, meet DESC, n_neighbors DESC, ends_soon ASC
"""

CAT_INFO = """
SELECT category3_id as category_id, category1_name, category2_name, category3_name, category
FROM categories
"""

GROUP_INFO = """
SELECT groups.group_id, is_online, address, monday, tuesday, wednesday, thursday, friday, saturday, sunday
FROM groups
LEFT JOIN group_timetable ON groups.group_id = group_timetable.group_id
WHERE finish_date > '2023-03-01'
ORDER BY finish_date ASC
"""


HISTORY = """
SELECT 
	category1_name
	, category2_name
	, category3_name
	, min(start_date) as start_date
	, max(finish_date) as finish_date
	, sum(n_visits) as n_visits
FROM attend_history
LEFT JOIN groups ON attend_history.group_id = groups.group_id
LEFT JOIN categories ON groups.category3_id = categories.category3_id
WHERE user_id = ?
GROUP BY category1_name
	, category2_name
	, category3_name
ORDER BY start_date DESC, finish_date DESC
"""


QUESTIONNAIRE_GROUPS = """
SELECT 
	group_suggest.group_id 
	, is_online 
	, category3_id
	, same_district 
	, same_zone
	, same_post
	, n_neighbors
	, total_rank
FROM (
	SELECT 
		groups.group_id
		, max(is_online) as is_online
		, min(category3_id) as category3_id
		, max(ends_soon) as ends_soon
		, coalesce(g_district, 0) = u_district AND u_district > 0 as same_district
		, coalesce(g_zone, 0) = u_zone AND u_zone > 0 as same_zone
		, coalesce(g_postal_code, 0) = u_postal AND u_postal > 0 as same_post
		, coalesce(n_neighbors, 0) as n_neighbors
	FROM groups 
	INNER JOIN group_timetable ON groups.group_id = group_timetable.group_id
	LEFT JOIN group_locations ON groups.group_id = group_locations.group_id
	LEFT JOIN (
		SELECT code as u_postal, coalesce(zone_id, 0) as u_zone, coalesce(district_id, 0) as u_district
		FROM postal_map
		WHERE code = ?
	) users ON 1=1
	LEFT JOIN model_neighbors ON groups.group_id = model_neighbors.group_id AND users.u_postal = model_neighbors.postal_code 
	WHERE is_available = 1
	GROUP BY groups.group_id
) as group_suggest
LEFT JOIN (
	SELECT 
		base.category3_id as category_out
		, - coalesce(rank_age, 200) as total_rank
	FROM (
		SELECT category3_id
		FROM categories
	) as base 
	LEFT JOIN (
		SELECT category_out, rank_age
		FROM model_age
		WHERE age_group = ? AND is_woman = ?
		ORDER BY rank_age ASC
	) rank_age ON base.category3_id = rank_age.category_out
	INNER JOIN (
		SELECT DISTINCT category3_id, 1 as is_available
		FROM groups
		WHERE is_available = 1
	) group_availability ON base.category3_id = group_availability.category3_id
	-- ORDER BY total_rank DESC
) as category_suggest ON group_suggest.category3_id = category_suggest.category_out
WHERE ends_soon = 0
ORDER BY total_rank DESC, same_district DESC, (same_zone + (n_neighbors > 5)) DESC, same_post DESC, n_neighbors DESC, ends_soon ASC
"""

QUESTIONNAIRE_CAT = """
SELECT category3_id, feature
FROM questionnaire
"""