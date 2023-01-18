Bienvenidos
```SQL
SELECT tbl_votes.PostId as `Post Id`, tbl_post.Score as Question, COUNT(tbl_votes.PostId) as `Vote Count`
FROM tbl_votes 
INNER JOIN tbl_post ON tbl_votes.PostId = tbl_post.Id_post 
WHERE tbl_post.Id_posttype = 2 AND tbl_votes.VoteTypeId = 2 
GROUP BY tbl_votes.PostId, tbl_post.Score
ORDER BY COUNT(tbl_votes.PostId) DESC
;
```

```SCALA

 val votesPostsJoinDf = votesDf.as("Votes")
        .join(postsDfClean.as("Posts"), votesPostsJoinCondition, "inner")
    
      votesPostsJoinDf
        .filter("Posts.PostTypeId = 2 and Votes.VoteTypeId = 2")
        .groupBy("Votes.PostId", "Posts.Body")
        .agg(
          count("Votes.PostId").as("Vote Count")
        )
        .withColumnRenamed("Body", "Question")
        .withColumnRenamed("PostId", "Post Id")
        .orderBy(desc("Vote Count"))
        .show()
        
```


