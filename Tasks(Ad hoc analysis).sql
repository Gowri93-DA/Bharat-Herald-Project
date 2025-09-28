/*  1: Monthly Circulation Drop Check 

WITH circulation AS (
    SELECT 
        c.city AS city_name,
        f.Year,
        f.Month_Num,
        f.Net_Circulation
    FROM fact_print_sales f
    JOIN dim_city c 
      ON f.city_id = c.city_id
    WHERE f.Year BETWEEN 2019 AND 2024
),
diffs AS (
    SELECT 
        city_name,
        Year,
        Month_Num,
        Net_Circulation,
        (Net_Circulation - 
            LAG(Net_Circulation) OVER (
                PARTITION BY city_name 
                ORDER BY Year, Month_Num)
        ) AS month_change
    FROM circulation
)
SELECT 
    city_name,
    CONCAT(Year,'-',LPAD(Month_Num,2,'0')) AS month,
    Net_Circulation,
    month_change
FROM diffs
WHERE month_change < 0  -- only declines
ORDER BY month_change ASC  -- largest negative first
LIMIT 3;




/* 2: Yearly Revenue Concentration by Category


WITH yearly_totals AS (
    SELECT 
        year,
        SUM(ad_revenue) AS total_revenue_year
    FROM fact_ad_revenue
    GROUP BY year
),
category_revenue AS (
    SELECT 
        f.year,
        d.standard_ad_category AS category_name,
        SUM(f.ad_revenue) AS category_revenue,
        y.total_revenue_year,
        ROUND(
            (SUM(f.ad_revenue) * 100.0 / y.total_revenue_year), 2
        ) AS pct_of_year_total
    FROM fact_ad_revenue f
    JOIN dim_ad_category d 
        ON f.ad_category_id = d.ad_category_id
    JOIN yearly_totals y
        ON f.year = y.year
    GROUP BY f.year, d.standard_ad_category, y.total_revenue_year
),
ranked AS (
    SELECT 
        year,
        category_name,
        category_revenue,
        total_revenue_year,
        pct_of_year_total,
        RANK() OVER (PARTITION BY year ORDER BY category_revenue DESC) AS category_rank
    FROM category_revenue
)
SELECT 
    year,
    category_name,
    category_revenue,
    total_revenue_year,
    pct_of_year_total,
    CASE 
        WHEN pct_of_year_total > 50 THEN 'Yes'
        ELSE 'No'
    END AS exceeds_50_pct
FROM ranked
WHERE category_rank = 1
ORDER BY year;


/* 3: 2024 Print Efficiency Leaderboard

WITH city_totals_2024 AS (
    SELECT 
        f.city_id,
        SUM(f.`Copies Sold` + f.copies_returned) AS copies_printed_2024,
        SUM(f.net_circulation) AS net_circulation_2024
    FROM fact_print_sales f
    WHERE f.Year = 2024   -- ✅ use the Year column directly
    GROUP BY f.city_id
),
city_efficiency AS (
    SELECT 
        c.city AS city_name,
        t.copies_printed_2024,
        t.net_circulation_2024,
        ROUND(
            t.net_circulation_2024 * 1.0 / NULLIF(t.copies_printed_2024,0), 4
        ) AS efficiency_ratio
    FROM city_totals_2024 t
    JOIN dim_city c
      ON t.city_id = c.city_id
),
ranked AS (
    SELECT 
        city_name,
        copies_printed_2024,
        net_circulation_2024,
        efficiency_ratio,
        RANK() OVER (ORDER BY efficiency_ratio DESC) AS efficiency_rank_2024
    FROM city_efficiency
)
SELECT 
    city_name,
    copies_printed_2024,
    net_circulation_2024,
    efficiency_ratio,
    efficiency_rank_2024
FROM ranked
WHERE efficiency_rank_2024 <= 5
ORDER BY efficiency_rank_2024;


/* 4 : Internet Readiness Growth (2021)

WITH q1 AS (
    SELECT 
        f.city_id,
        AVG(f.internet_penetration) AS internet_rate_q1_2021
    FROM fact_city_readiness f
    WHERE f.year = 2021 AND f.quarter = 'Q1'
    GROUP BY f.city_id
),
q4 AS (
    SELECT 
        f.city_id,
        AVG(f.internet_penetration) AS internet_rate_q4_2021
    FROM fact_city_readiness f
    WHERE f.year = 2021 AND f.quarter = 'Q4'
    GROUP BY f.city_id
),
combined AS (
    SELECT 
        c.city AS city_name,
        q1.internet_rate_q1_2021,
        q4.internet_rate_q4_2021,
        ROUND(q4.internet_rate_q4_2021 - q1.internet_rate_q1_2021,3) AS delta_internet_rate
    FROM q1
    JOIN q4 ON q1.city_id = q4.city_id
    JOIN dim_city c ON q1.city_id = c.city_id
)
SELECT 
    city_name,
    internet_rate_q1_2021,
    internet_rate_q4_2021,
    delta_internet_rate
FROM combined
ORDER BY delta_internet_rate DESC
;  -- city with the highest improvement


/* 5: Consistent Multi-Year Decline (2019→2024)  

/*  Map edition_id /* 1. Map edition_id to city_id */
WITH edition_city_map AS (
    SELECT DISTINCT edition_id, city_id
    FROM fact_print_sales
),

/*  Get yearly net_circulation per city */
yearly_print AS (
    SELECT 
        c.city_id,
        c.city AS city_name,
        f.year,
        SUM(f.net_circulation) AS yearly_net_circulation
    FROM fact_print_sales f
    JOIN dim_city c ON f.city_id = c.city_id
    WHERE f.year IN (2019, 2024)
    GROUP BY c.city_id, c.city, f.year
),

/*  Get yearly ad_revenue per city using edition_id mapping */
yearly_ad AS (
    SELECT 
        m.city_id,
        ar.year,
        SUM(ar.ad_revenue) AS yearly_ad_revenue
    FROM fact_ad_revenue ar
    JOIN edition_city_map m ON ar.edition_id = m.edition_id
    WHERE ar.year IN (2019, 2024)
    GROUP BY m.city_id, ar.year
),

/*  Combine both */
yearly AS (
    SELECT 
        p.city_id,
        p.city_name,
        p.year,
        p.yearly_net_circulation,
        a.yearly_ad_revenue
    FROM yearly_print p
    LEFT JOIN yearly_ad a 
      ON p.city_id = a.city_id AND p.year = a.year
),

/*  Pivot 2019 and 2024 to compare directly */
pivoted AS (
    SELECT 
        city_id,
        city_name,
        MAX(CASE WHEN year = 2019 THEN yearly_net_circulation END) AS net_2019,
        MAX(CASE WHEN year = 2024 THEN yearly_net_circulation END) AS net_2024,
        MAX(CASE WHEN year = 2019 THEN yearly_ad_revenue END) AS ad_2019,
        MAX(CASE WHEN year = 2024 THEN yearly_ad_revenue END) AS ad_2024
    FROM yearly
    GROUP BY city_id, city_name
)

/*  Final output */
SELECT 
    city_name,
    net_2019 AS yearly_net_circulation_2019,
    net_2024 AS yearly_net_circulation_2024,
    ad_2019 AS yearly_ad_revenue_2019,
    ad_2024 AS yearly_ad_revenue_2024,
    CASE WHEN net_2024 < net_2019 THEN 'Yes' ELSE 'No' END AS is_declining_print,
    CASE WHEN ad_2024 < ad_2019 THEN 'Yes' ELSE 'No' END AS is_declining_ad_revenue,
    CASE 
        WHEN net_2024 < net_2019 AND ad_2024 < ad_2019 THEN 'Yes'
        ELSE 'No'
    END AS is_declining_both
FROM pivoted
ORDER BY city_name;




/*  6 : 2021 Readiness vs Pilot Engagement Outlier 

WITH readiness AS (
    /* Average the three readiness factors across all 2021 quarters per city */
    SELECT 
        cr.city_id,
        ROUND(AVG(
            (cr.smartphone_penetration + cr.internet_penetration + cr.literacy_rate)/3.0
        ),2) AS readiness_score_2021
    FROM fact_city_readiness cr
    WHERE cr.year = 2021
    GROUP BY cr.city_id
),
ranked_readiness AS (
    SELECT 
        city_id,
        readiness_score_2021,
        RANK() OVER (ORDER BY readiness_score_2021 DESC) AS readiness_rank_desc
    FROM readiness
),
engagement AS (
    /* Sum all users_reached for 2021 per city */
    SELECT 
        dp.city_id,
        SUM(dp.users_reached) AS engagement_metric_2021
    FROM fact_digital_pilot dp
    WHERE dp.year = 2021  -- adjust if your year column named differently
    GROUP BY dp.city_id
),
ranked_engagement AS (
    SELECT 
        city_id,
        engagement_metric_2021,
        RANK() OVER (ORDER BY engagement_metric_2021 ASC) AS engagement_rank_asc
    FROM engagement
)
SELECT 
    dc.city AS city_name,
    rr.readiness_score_2021,
    re.engagement_metric_2021,
    rr.readiness_rank_desc,
    re.engagement_rank_asc,
    CASE 
      WHEN rr.readiness_rank_desc = 1 AND re.engagement_rank_asc <= 3 THEN 'Yes'
      ELSE 'No'
    END AS is_outlier
FROM ranked_readiness rr
JOIN ranked_engagement re ON rr.city_id = re.city_id
JOIN dim_city dc ON rr.city_id = dc.city_id
ORDER BY rr.readiness_rank_desc, re.engagement_rank_asc;





