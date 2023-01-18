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

```SQL
SELECT tbl_users.DisplayName as Username, tbl_post.Id_OwnerUser as `User Id`, COUNT(tbl_votes.Id) as `Bounties Won`
FROM tbl_votes 
INNER JOIN tbl_post ON tbl_votes.PostId = tbl_post.Id_post 
INNER JOIN tbl_users ON tbl_post.Id_OwnerUser = tbl_users.Id_user
WHERE tbl_votes.VoteTypeId = 9 
GROUP BY tbl_post.Id_OwnerUser, tbl_users.DisplayName 
ORDER BY COUNT(tbl_votes.Id) DESC
Limit 10;

```

```SCALA
    val votesDf = spark.read.parquet("src/main/resources/Votes.parquet")
    val votesPostsJoinCondition = col("Votes.PostId") === col("Posts.Id")
    val votesUsersPostsJoinDf = votesDf.as("Votes")
      .join(postsDfClean.as("Posts"), votesPostsJoinCondition, "inner")
      .join(usersDf.as("Users"), postsUsersJoinCondition, "inner")
    
    votesUsersPostsJoinDf
      .filter("Votes.VoteTypeId = 9")
      .groupBy("Posts.OwnerUserId", "Users.DisplayName")
      .agg(
        count("Votes.Id").as("Bounties Won")
      )
      .withColumnRenamed("DisplayName", "Username")
      .withColumnRenamed("OwnerUserId", "User Id")
      .orderBy(desc("Bounties Won"))
      .show()
```





