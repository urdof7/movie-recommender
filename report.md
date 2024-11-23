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
```

---

## Observations on Filtering Methods

### Content-Based Filtering

- **Pros**:
  - Highly interpretable: Recommendations are derived from explicit movie features like genres, directors, and cast.
  - Effective for users with well-defined preferences (i.e., those who have rated movies highly).

- **Cons**:
  - **Applicability Gap**: Fails for users without highly rated movies (approximately 18% of users in the dataset).
  - **Limited Diversity**: Recommendations are often confined to movies similar to those the user has already rated, potentially reducing novelty.

### Collaborative Filtering

- **Pros**:
  - Captures latent patterns in user preferences, enabling diverse and broader recommendations.
  - Does not rely on explicit feature data, making it universally applicable for all users, regardless of their individual high-rating history.

- **Cons**:
  - Requires substantial user-item interaction data for accuracy.
  - Less effective in handling new users or items with sparse ratings.

---

## Conclusion

Content-based filtering, while transparent and interpretable, is limited by its inability to serve users without high ratings. Collaborative filtering, on the other hand, offers broader applicability and greater diversity in recommendations, making it more suitable for datasets with mixed user engagement.

Given that approximately **18% of users** are not served by content-based filtering under the current threshold (we could lower the 4.0 threshold but it wouldn't make sense to recommend movies based on similarity to a movie the user rated low), it may be worth considering a hybrid approach. Such a system could leverage collaborative filtering for users without high ratings and content-based filtering for users with clear preference profiles.
