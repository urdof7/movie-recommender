# Analysis of Content-Based and Collaborative Filtering Approaches

---

## Introduction

Content-based filtering and collaborative filtering are two primary methods for generating personalized recommendations. This report provides a concise comparison of these approaches, highlighting their strengths, limitations, and applicability based on experimental observations.

---

## Key Observations

- **Average Number of Movies Rated per User**: Approximately **3.7 movies** per user in the dataset.

- **Limitations of Content-Based Filtering**:
  - Content-based filtering requires users to have rated at least one movie with a score of **4.0 or higher** to generate recommendations.
  - Approximately **18% of users** in the dataset have not rated any movie 4.0 or higher, making content-based filtering inapplicable for these users in its current form.

---

## SQL Query to Determine the Percentage of Affected Users

```sql
WITH total_users AS (
    SELECT COUNT(DISTINCT user_id) AS total_user_count
    FROM rating
),
users_without_high_ratings AS (
    SELECT COUNT(DISTINCT user_id) AS low_rating_user_count
    FROM rating
    WHERE user_id NOT IN (
        SELECT user_id
        FROM rating
        GROUP BY user_id
        HAVING MAX(rating) >= 4.0
    )
)
SELECT 
    (users_without_high_ratings.low_rating_user_count * 100.0) / total_users.total_user_count AS percentage_without_high_ratings
FROM 
    total_users
JOIN 
    users_without_high_ratings ON 1=1;
