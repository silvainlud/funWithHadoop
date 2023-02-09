# David and Goliath 

```sql
select "D", count(id)
from chess2
where victory_status == "mate"
  and white_rating < black_rating
UNION
select "G", count(id)
from chess2
where victory_status == "mate"
  and white_rating > black_rating;
```

# Best Opening

```sql
select T.opening_name, T.rating, count(*)
from (select opening_name, greatest(white_rating, black_rating) as rating from chess2) as T
group by T.opening_name, T.rating
order by count(*) desc
LIMIT 10;
```