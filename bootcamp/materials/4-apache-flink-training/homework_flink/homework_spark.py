from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, desc, broadcast
from pyspark.sql.window import Window

def job(spark):
    # Read data with explicit schema and options
    match_details = spark.read.option("header", "true").option("inferSchema", "true") \
        .csv("/home/iceberg/data/match_details.csv")
    matches = spark.read.option("header", "true").option("inferSchema", "true") \
        .csv("/home/iceberg/data/matches.csv")
    medals_matches_players = spark.read.option("header", "true").option("inferSchema", "true") \
        .csv("/home/iceberg/data/medals_matches_players.csv")
    medals = spark.read.option("header", "true").option("inferSchema", "true") \
        .csv("/home/iceberg/data/medals.csv")
    maps = spark.read.option("header", "true").option("inferSchema", "true") \
        .csv("/home/iceberg/data/maps.csv")

    # Disable automatic broadcast join
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

    # Broadcast small reference tables
    medals_broadcast = broadcast(medals)
    maps_broadcast = broadcast(maps)

    # Create database and use it
    spark.sql("CREATE DATABASE IF NOT EXISTS homework")
    spark.sql("USE homework")

    # Create bucketed Iceberg tables with proper partitioning
    tables_to_bucket = [
        {
            "name": "match_details_bucketed",
            "schema": """
            CREATE TABLE IF NOT EXISTS match_details_bucketed (
                match_id STRING,
                player_gamertag STRING,
                player_total_kills INT
            )
            USING iceberg
            PARTITIONED BY (match_id)
            CLUSTERED BY (match_id) INTO 16 BUCKETS
            """,
            "df": match_details.select(
                "match_id", 
                "player_gamertag", 
                col("player_total_kills").cast("int")
            )
        },
        {
            "name": "matches_bucketed",
            "schema": """
            CREATE TABLE IF NOT EXISTS matches_bucketed (
                match_id STRING,
                mapid STRING,
                map_variant_id STRING,
                is_team_game BOOLEAN,
                playlist_id STRING,
                is_match_over BOOLEAN,
                completion_date TIMESTAMP
            )
            USING iceberg
            PARTITIONED BY (match_id)
            CLUSTERED BY (match_id) INTO 16 BUCKETS
            """,
            "df": matches.select(
                "match_id", 
                "mapid", 
                "map_variant_id", 
                col("is_team_game").cast("boolean"), 
                "playlist_id", 
                col("is_match_over").cast("boolean"), 
                "completion_date"
            )
        },
        {
            "name": "medals_matches_players_bucketed",
            "schema": """
            CREATE TABLE IF NOT EXISTS medals_matches_players_bucketed (
                match_id STRING,
                player_gamertag STRING,
                medal_id STRING,
                count INT
            )
            USING iceberg
            PARTITIONED BY (match_id)
            CLUSTERED BY (match_id) INTO 16 BUCKETS
            """,
            "df": medals_matches_players.select(
                "match_id", 
                "player_gamertag", 
                "medal_id", 
                col("count").cast("int")
            )
        }
    ]

    # Create and populate bucketed tables
    for table in tables_to_bucket:
        # Drop table if exists
        spark.sql(f"DROP TABLE IF EXISTS {table['name']}")
        
        # Create table with schema
        spark.sql(table['schema'])
        
        # Write data to table
        table['df'].write.mode("overwrite") \
            .saveAsTable(table['name'])

    # Perform optimized join
    result = spark.sql("""
        SELECT 
            mdb.match_id, 
            mdb.player_gamertag, 
            mdb.player_total_kills, 
            mb.mapid, 
            mb.map_variant_id, 
            mb.is_team_game, 
            mb.playlist_id, 
            mb.is_match_over, 
            mb.completion_date,
            mmpb.medal_id, 
            mmpb.count as medal_count
        FROM match_details_bucketed mdb
        JOIN matches_bucketed mb ON mdb.match_id = mb.match_id
        JOIN medals_matches_players_bucketed mmpb 
            ON mdb.match_id = mmpb.match_id 
            AND mdb.player_gamertag = mmpb.player_gamertag
    """)

    # Optimize with broadcast joins for small tables
    result_with_maps = result.join(maps_broadcast, on="mapid", how="inner")
    result_with_maps_medals = result_with_maps.join(medals_broadcast, on="medal_id", how="inner")

    # 1. Player with most average kills
    avg_kills = result_with_maps_medals.groupBy("player_gamertag") \
        .agg(avg("player_total_kills").alias("avg_kills")) \
        .orderBy(desc("avg_kills")) \
        .limit(1)
    
    # 2. Most played playlists 
    playlist_counts = result_with_maps_medals.groupBy("playlist_id") \
        .agg(count("*").alias("play_count")) \
        .orderBy(desc("play_count")) \
        .limit(1)

    # 3. Most played maps
    map_counts = result_with_maps_medals.groupBy("name") \
        .agg(count("*").alias("play_count")) \
        .orderBy(desc("play_count")) \
        .limit(1)
    
    # 4. Maps with most Killing Spree medals
    killing_spree_maps = result_with_maps_medals.filter(col("medal_name") == "Killing Spree") \
        .groupBy("name", "medal_name") \
        .agg(count("*").alias("killing_spree_count")) \
        .orderBy(desc("killing_spree_count")) \
        .limit(1)

    # Optimization strategies
    optimization_strategies = [
        result_with_maps_medals.sortWithinPartitions("playlist_id"),
        result_with_maps_medals.sortWithinPartitions("mapid"),
        result_with_maps_medals.sortWithinPartitions("player_gamertag")
    ]

    # Compute partition sizes
    partition_sizes = [
        sum(df.rdd.glom().map(lambda p: len(p)).collect())
        for df in optimization_strategies
    ]

    return (
        avg_kills, 
        playlist_counts, 
        map_counts, 
        killing_spree_maps, 
        partition_sizes
    )

def main():
    # Initialize Spark session with optimized configurations
    spark = SparkSession.builder \
        .appName("OptimizedGameAnalytics") \
        .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
        .config("spark.sql.bucketedJoin.enabled", "true") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.sql.shuffle.partitions", "16") \
        .getOrCreate()

    try:
        # Execute job
        avg_kills, playlist_counts, map_counts, killing_spree_maps, partition_sizes = job(spark)

        # Log results using Spark's logging mechanism
        print("\nTop Players by Average Kills:")
        avg_kills.show()

        print("\nMost Played Playlists:")
        playlist_counts.show()

        print("\nMost Played Maps:")
        map_counts.show()

        print("\nMaps with Most Killing Spree Medals:")
        killing_spree_maps.show()

        print("\nPartition Sizes for Different Sorting Strategies:")
        for i, size in enumerate(partition_sizes):
            print(f"Strategy {i+1} size: {size}")

    finally:
        spark.stop()

if __name__ == "__main__":
    main()