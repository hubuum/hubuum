SELECT (SELECT retained FROM auth_interaction_capacity WHERE singleton) AS total,
    count(*) AS active, count(*) FILTER (WHERE client_id=$1) AS client, count(*) FILTER (WHERE actor_id=$2) AS actor
    FROM auth_interactions WHERE active_until>$3 AND created_at<=$3
